"""Custom client handling, including BraintreeStream base class."""
import braintree
import pytz
from flatten_json import flatten
from datetime import datetime, timedelta, date
from dateutil.parser import isoparse

import types
from decimal import Decimal
from braintree import Descriptor, RiskData
from braintree.disbursement_detail import DisbursementDetail
from braintree.transaction_details import TransactionDetails

from typing import Any, Dict, Optional, Union, List, Iterable

from requests.exceptions import ReadTimeout
from singer_sdk.streams import Stream
import copy


class BraintreeStream(Stream):
    """Stream class for braintree2 streams."""

    @property
    def braintree_objects(self):
        return Descriptor, DisbursementDetail, RiskData, TransactionDetails

    @property
    def fetch_records_interval_days(self):
        return self.config.get("fetch_records_interval_days", 1)

    @property
    def start_date(self):
        return self.config["start_date"]

    @property
    def global_stream_state(self):
        return self.config.get("global_stream_state", "start_date")

    @property
    def braintree_config_merchant_id(self) -> dict:
        return {
            'merchant_id': self.config["merchant_id"],
            'public_key': self.config["public_key"],
            'private_key': self.config["private_key"],
        }

    @staticmethod
    def date_range(start_date, end_date, interval_in_days=1):
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

        start_date = (datetime.combine(start_date.date(), datetime.min.time())
                      - timedelta(interval_in_days)).replace(tzinfo=pytz.UTC)
        end_date = (end_date + timedelta(1)).replace(tzinfo=pytz.UTC)

        for n in range(0, int((end_date - start_date).days), interval_in_days):
            yield start_date + timedelta(n), start_date + timedelta(n + interval_in_days)

    def check_api_result_limits(self, results):
        try:
            if self.name == 'transactions':
                assert results.maximum_size < 50000
            else:
                assert results.maximum_size < 10000
        except AssertionError as e:
            self.logger.error(" ERROR: {} stream exceeded maximum records from API".format(self.name,
                                                                                           results.maximum_size))

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
            if hasattr(d, attr) and isinstance(getattr(d, attr), (list, set, tuple, types.GeneratorType)):
                child_obj_list = []
                # self.logger.info('array: \n{}'.format(getattr(d, attr)))
                for obj in getattr(d, attr):
                    if isinstance(obj, dict):
                        child_obj_list.append(flatten(self.object_to_dict(obj, ignore_obj, level=level)))
                    else:
                        child_obj_list.append(self.object_to_dict(obj, ignore_obj, level=level))
                if len(child_obj_list) > 0:
                    array_attr[attr] = child_obj_list

            elif hasattr(d, attr) and isinstance(getattr(d, attr), Decimal):
                flat_attr[attr] = float(getattr(d, attr))
            elif hasattr(d, attr) and isinstance(getattr(d, attr), datetime):
                flat_attr[attr] = str(getattr(d, attr).replace(tzinfo=pytz.UTC))
            elif hasattr(d, attr) and isinstance(getattr(d, attr), date):
                value = getattr(d, attr)
                flat_attr[attr] = str(datetime(value.year, value.month, value.day, tzinfo=pytz.UTC))
            elif hasattr(d, attr) and isinstance(getattr(d, attr), self.braintree_objects):
                flat_attr[attr] = self.object_to_dict(getattr(d, attr), ignore_obj, level=level)
                # pass
            elif hasattr(d, attr):
                # if level > 1: self.logger.info('default: \n{}'.format(getattr(d, attr)))
                flat_attr[attr] = getattr(d, attr)
            else:
                return

        flat_attr = flatten(flat_attr, root_keys_to_ignore=ignore_obj)
        flat_attr.update(array_attr)

        return flat_attr

    def contains_latest_record(self, record, last_updated):
        if getattr(record, 'updated_at') > last_updated:
            return True

        if hasattr(record, 'status_history'):
            sh = getattr(record, 'status_history')
            if len(sh) and hasattr(sh[-1], 'updated_at') and getattr(sh[-1], 'timestamp') > last_updated:
                return True

        if hasattr(record, 'disputes'):
            d = getattr(record, 'disputes')
            if len(d) and hasattr(d[-1], 'updated_at') and getattr(d[-1], 'updated_at') > last_updated:
                return True

        if hasattr(record, 'discounts'):
            d = getattr(record, 'discounts')
            if len(d) and hasattr(d[-1], 'updated_at') and getattr(d[-1], 'updated_at') > last_updated:
                return True
        return False

    def parse_record(self, record) -> dict:
        json_obj = {'disputes', 'status_history', 'discounts',
                    'risk_data_decision_reasons', 'refund_ids', 'refund_global_ids'}
        ignore_obj = {'transactions'}

        data = self.object_to_dict(record, ignore_obj)
        return data

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        self.logger.info(f" tap_states: {self.tap_state}")

        self.set_braintree_config()
        start_timestamp = self.get_starting_timestamp(context) or isoparse(self.start_date)
        end_timestamp = datetime.utcnow()

        state_dict = self.get_context_state(context)
        self.logger.info(f" state_dict: {state_dict}")
        self.logger.info(f" tap_states: {self.tap_state}")

        """  
        TODO: 
            Come up with a better way to make this work as state isn't working because multi-key state is unsupported
            from one stream and because braintree won't let you query by updated_at for all the needed keys
        """
        last_updated = datetime.strptime(self.global_stream_state, '%Y-%m-%d')

        for start, end in self.date_range(start_timestamp, end_timestamp,
                                          interval_in_days=self.fetch_records_interval_days):
            try:
                records = self.braintree_obj.search(self.braintree_search.between(start, end))
                self.check_api_result_limits(records)
                max_records_expected = records.maximum_size
                self.logger.info(" {}: Fetched {} records from {} - {}".format(self.name, max_records_expected,
                                                                               start.date(), end.date()))

                processed_count = 0
                self.logger.info(f"last_updated: {last_updated}")
                for record in records:
                    if self.contains_latest_record(record, last_updated):
                        processed_count += 1
                        yield self.parse_record(record)

            except (ConnectionError, ReadTimeout) as e:
                self.logger.error(" {}: Failed to process records from {} - {}".format(self.name,
                                                                                       start.date(),
                                                                                       end.date(),
                                                                                       ))
                self.logger.error(f" Exception: {str(e)}")
                self.logger.error(f" Exception occurred while processing record:\n{record}")
                break

            self.logger.info(" {}: Processed {} of {} records at {}".format(self.name,
                                                                            processed_count,
                                                                            max_records_expected,
                                                                            datetime.utcnow()))

