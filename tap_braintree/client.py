"""Custom client handling, including BraintreeStream base class."""
import braintree
import pytz
import time
from flatten_json import flatten
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from dateutil.parser import isoparse

import types
from decimal import Decimal
from braintree import Descriptor, RiskData
from braintree.disbursement_detail import DisbursementDetail
from braintree.transaction_details import TransactionDetails

from typing import Any, Dict, Optional, Union, List, Iterable

from requests.exceptions import ReadTimeout
from singer_sdk.streams import Stream


class BraintreeStream(Stream):
    """Stream class for braintree2 streams."""

    @property
    def braintree_objects(self):
        return Descriptor, DisbursementDetail, RiskData, TransactionDetails

    @property
    def fetch_records_interval_hours(self):
        return self.config.get("fetch_records_interval_hours", 24)

    @property
    def start_date(self):
        # All of this logic is a workaround to how slow the Braintree API can be. We only
        # need the last week or so of transactions because within that time period, their
        # status would update and shouldn't update again after that. For subscriptions,
        # things get a bit more complicated. On a daily basis, we only really need to fetch
        # the last month of data, because that would capture daily trials started/converted.
        # But, since we care about the Trial-to-Active rate for subscriptions, there's additional
        # logic allowing for a weekly sync (grabs the last 3 months) and full sync of subscriptions
        # data. Note, to cut down on run time, the full sync of subscriptions actually only
        # fetches the previous month's data for each year in the time since the start_date.
        if self.config["sync_state"] == "regular":
            if self.name == "subscriptions":
                return str(datetime.now() - relativedelta(months=1))
            elif self.name == "transactions":
                return str(datetime.now() - relativedelta(days=10))
            else:
                return self.config["start_date"]
        elif self.config["sync_state"] == "last 3 months":
            return str(datetime.now() - relativedelta(months=3))
        elif self.config["sync_state"] == "full":
            return self.config["start_date"]

    @property
    def global_stream_state(self):
        return self.config.get("global_stream_state", "start_date")

    @property
    def braintree_config_merchant_id(self) -> dict:
        return {
            "merchant_id": self.config["merchant_id"],
            "public_key": self.config["public_key"],
            "private_key": self.config["private_key"],
        }

    @staticmethod
    def date_range(start_date, end_date, interval_in_hours=24):
        """
        Generator function that produces an iterable list of days between the two
        dates start_date and end_date as a tuple pair of datetimes.

        Args:
            start_date (datetime): start of period
            end_date (datetime): end of period
            interval_in_days (int): interval of days to iter over

        Yields:
            tuple: daily period
                * datetime: day within range - interval_in_days
                * datetime: day within range + interval_in_days

        """
        current_date = start_date
        while current_date < end_date:
            interval_start = current_date
            interval_end = current_date + timedelta(hours=interval_in_hours)

            if interval_end > end_date:
                interval_end = end_date

            yield interval_start, interval_end
            current_date = interval_end

    def check_api_result_limits(self, results):
        try:
            if self.name == "transactions":
                assert results.maximum_size < 50000
            else:
                assert results.maximum_size < 10000
        except AssertionError as e:
            self.logger.error(
                " ERROR: {} stream exceeded maximum records from API".format(
                    self.name, results.maximum_size
                )
            )

    def set_braintree_config(self):
        config = self.braintree_config_merchant_id
        environment = getattr(braintree.Environment, "Production")
        return braintree.Configuration.configure(environment, **config)

    def object_to_dict(self, d, ignore_obj, level=0) -> dict:
        level += 1
        flat_attr = dict()
        array_attr = dict()

        try:
            attributes = d._setattrs
        except AttributeError as e:
            return d

        for attr in attributes:
            if attr == "addresses" and hasattr(d, attr):
                # Get the first address with a non-None country_code_alpha2
                addresses = getattr(d, attr)
                if addresses and len(addresses) > 0:
                    valid_address = next(
                        (addr for addr in addresses if hasattr(addr, "country_code_alpha2") 
                         and getattr(addr, "country_code_alpha2") is not None),
                        addresses[0]  # Fallback to first address if none found
                    )
                    # Prefix address fields to avoid conflicts
                    for address_attr in valid_address._setattrs:
                        if hasattr(valid_address, address_attr):
                            flat_attr[f"address_{address_attr}"] = getattr(valid_address, address_attr)
                continue
            if hasattr(d, attr) and isinstance(
                getattr(d, attr), (list, set, tuple, types.GeneratorType)
            ):
                child_obj_list = []
                for obj in getattr(d, attr):
                    if isinstance(obj, dict):
                        child_obj_list.append(
                            flatten(self.object_to_dict(obj, ignore_obj, level=level))
                        )
                    else:
                        child_obj_list.append(
                            self.object_to_dict(obj, ignore_obj, level=level)
                        )
                if len(child_obj_list) > 0:
                    array_attr[attr] = child_obj_list

            elif hasattr(d, attr) and isinstance(getattr(d, attr), Decimal):
                flat_attr[attr] = float(getattr(d, attr))
            elif hasattr(d, attr) and isinstance(getattr(d, attr), datetime):
                flat_attr[attr] = str(getattr(d, attr).replace(tzinfo=pytz.UTC))
            elif hasattr(d, attr) and isinstance(getattr(d, attr), date):
                value = getattr(d, attr)
                flat_attr[attr] = str(
                    datetime(value.year, value.month, value.day, tzinfo=pytz.UTC)
                )
            elif hasattr(d, attr) and isinstance(
                getattr(d, attr), self.braintree_objects
            ):
                flat_attr[attr] = self.object_to_dict(
                    getattr(d, attr), ignore_obj, level=level
                )
            elif hasattr(d, attr):
                flat_attr[attr] = getattr(d, attr)
            else:
                return

        flat_attr = flatten(flat_attr, root_keys_to_ignore=ignore_obj)
        flat_attr.update(array_attr)

        return flat_attr

    def contains_latest_record(self, record, last_updated):
        if getattr(record, "updated_at") > last_updated:
            return True

        if hasattr(record, "status_history"):
            sh = getattr(record, "status_history")
            if (
                len(sh)
                and hasattr(sh[-1], "updated_at")
                and getattr(sh[-1], "timestamp") > last_updated
            ):
                return True

        if hasattr(record, "disputes"):
            d = getattr(record, "disputes")
            if (
                len(d)
                and hasattr(d[-1], "updated_at")
                and getattr(d[-1], "updated_at") > last_updated
            ):
                return True

        if hasattr(record, "discounts"):
            d = getattr(record, "discounts")
            if (
                len(d)
                and hasattr(d[-1], "updated_at")
                and getattr(d[-1], "updated_at") > last_updated
            ):
                return True
        return False

    def parse_record(self, record) -> dict:
        json_obj = {
            "disputes",
            "status_history",
            "discounts",
            "risk_data_decision_reasons",
            "refund_ids",
            "refund_global_ids",
        }
        ignore_obj = {"transactions"}

        data = self.object_to_dict(record, ignore_obj)
        return data

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        self.logger.info(f" tap_states: {self.tap_state}")
        self.set_braintree_config()
        end_timestamp = datetime.utcnow().replace(tzinfo=pytz.UTC)
        
        # Logic for braintree subscriptions full sync.
        if self.config["sync_state"] == "full":
            start_date_config = isoparse(self.config["start_date"]).replace(tzinfo=pytz.UTC)
            last_month_date = end_timestamp - relativedelta(months=1)
            target_month = last_month_date.month

            # Build a list of month start dates (one per year) for the current month,
            # starting from the year of start_date_config until the previous year.
            months_to_sync = []
            for year in range(start_date_config.year, last_month_date.year):
                month_start = datetime(year, target_month, 1, tzinfo=pytz.UTC)
                if month_start < start_date_config:
                    continue
                if month_start > last_month_date:
                    break
                months_to_sync.append(month_start)

            self.logger.info(f"Full sync will process these month start dates: {months_to_sync}")

            for month_start in months_to_sync:
                month_end = month_start + relativedelta(months=1)
                if month_end > last_month_date:
                    month_end = last_month_date
                self.logger.info(f"Syncing data for period: {month_start} to {month_end}")

                for start, end in self.date_range(
                    month_start,
                    month_end,
                    interval_in_hours=self.fetch_records_interval_hours,
                ):
                    while True:
                        try:
                            records = self.braintree_obj.search(
                                self.braintree_search.between(start, end)
                            )
                            self.check_api_result_limits(records)
                            max_records_expected = records.maximum_size
                            self.logger.info(
                                " {}: Fetched {} records from {} - {}".format(
                                    self.name, max_records_expected, start, end
                                )
                            )

                            processed_count = 0
                            for record in records:
                                if self.contains_latest_record(record, datetime.strptime(self.global_stream_state, "%Y-%m-%d")):
                                    processed_count += 1
                                    yield self.parse_record(record)

                        except braintree.exceptions.down_for_maintenance_error.DownForMaintenanceError as e:
                            self.logger.error(f" Exception: {str(e)}")
                            self.logger.error("Waiting 1 hour, then trying again...")
                            time.sleep(3600)
                            continue

                        except (ConnectionError, ReadTimeout) as e:
                            self.logger.error(
                                " {}: Failed to process records from {} - {}".format(
                                    self.name,
                                    start.date(),
                                    end.date(),
                                )
                            )
                            self.logger.error(f" Exception: {str(e)}")
                            break

                        self.logger.info(
                            " {}: Processed {} of {} records at {}".format(
                                self.name,
                                processed_count,
                                max_records_expected,
                                datetime.utcnow(),
                            )
                        )
                        break

        else:
            start_timestamp = self.get_starting_timestamp(context) or isoparse(self.start_date)
            start_timestamp = start_timestamp.replace(tzinfo=pytz.UTC)

            for start, end in self.date_range(
                start_timestamp,
                end_timestamp,
                interval_in_hours=self.fetch_records_interval_hours,
            ):
                while True:
                    try:
                        records = self.braintree_obj.search(
                            self.braintree_search.between(start, end)
                        )
                        self.check_api_result_limits(records)
                        max_records_expected = records.maximum_size
                        self.logger.info(
                            " {}: Fetched {} records from {} - {}".format(
                                self.name, max_records_expected, start, end
                            )
                        )

                        processed_count = 0
                        for record in records:
                            if self.contains_latest_record(record, datetime.strptime(self.global_stream_state, "%Y-%m-%d")):
                                processed_count += 1
                                yield self.parse_record(record)

                    except braintree.exceptions.down_for_maintenance_error.DownForMaintenanceError as e:
                        self.logger.error(f" Exception: {str(e)}")
                        self.logger.error("Waiting 1 hour, then trying again...")
                        time.sleep(3600)
                        continue

                    except (ConnectionError, ReadTimeout) as e:
                        self.logger.error(
                            " {}: Failed to process records from {} - {}".format(
                                self.name,
                                start.date(),
                                end.date(),
                            )
                        )
                        self.logger.error(f" Exception: {str(e)}")
                        break

                    self.logger.info(
                        " {}: Processed {} of {} records at {}".format(
                            self.name,
                            processed_count,
                            max_records_expected,
                            datetime.utcnow(),
                        )
                    )
                    break
