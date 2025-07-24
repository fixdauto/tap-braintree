"""Stream type classes for tap-braintree."""

from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.typing import PropertiesList, Property
from singer_sdk.typing import (
    StringType,
    BooleanType,
    ObjectType,
    DateTimeType,
    ArrayType,
    NumberType,
    IntegerType,
)

import time
from datetime import datetime, timedelta, date
from pathlib import Path

import braintree
from tap_braintree.client import BraintreeStream


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class TransactionsStream(BraintreeStream):
    name = "transactions"
    primary_keys = ["id"]
    replication_key = None

    braintree_obj = braintree.Transaction
    braintree_search = braintree.TransactionSearch.created_at

    schema = PropertiesList(
        Property("id", StringType, required=True),
        Property("acquirer_reference_number", StringType),
        Property("additional_processor_response", StringType),
        Property("amount", NumberType),
        Property("authorization_expires_at", StringType),
        Property("avs_error_response_code", StringType),
        Property("avs_postal_code_response_code", StringType),
        Property("avs_street_address_response_code", StringType),
        Property("billing_company", StringType),
        Property("billing_country_code_alpha2", StringType),
        Property("billing_country_code_alpha3", StringType),
        Property("billing_country_code_numeric", StringType),
        Property("billing_country_name", StringType),
        Property("billing_extended_address", StringType),
        Property("billing_id", StringType),
        Property("billing_locality", StringType),
        Property("billing_postal_code", StringType),
        Property("billing_region", StringType),
        Property("billing_street_address", StringType),
        Property("channel", StringType),
        Property("created_at", DateTimeType),
        Property("android_pay_card_source_card_type", StringType),
        Property("apple_pay_card_type", StringType),
        Property("credit_card_bin", StringType),
        Property("credit_card_card_type", StringType),
        Property("credit_card_cardholder_name", StringType),
        Property("credit_card_commercial", StringType),
        Property("credit_card_country_of_issuance", StringType),
        Property("credit_card_customer_location", StringType),
        Property("credit_card_debit", StringType),
        Property("credit_card_durbin_regulated", StringType),
        Property("credit_card_expiration_month", StringType),
        Property("credit_card_expiration_year", StringType),
        Property("credit_card_global_id", StringType),
        Property("credit_card_healthcare", StringType),
        Property("credit_card_image_url", StringType),
        Property("credit_card_issuing_bank", StringType),
        Property("credit_card_last_4", StringType),
        Property("credit_card_payroll", StringType),
        Property("credit_card_prepaid", StringType),
        Property("credit_card_product_id", StringType),
        Property("credit_card_token", StringType),
        Property("credit_card_unique_number_identifier", StringType),
        Property("credit_card_venmo_sdk", BooleanType),
        Property("currency_iso_code", StringType),
        Property("custom_fields", StringType),
        Property("custom_fields_upsellery_billing_cycle", StringType),
        Property("custom_fields_upsellery_subscription_period_id", StringType),
        Property("custom_fields_upsellery_subscription_id", StringType),
        Property("custom_fields_upsellery_customer_id", StringType),
        Property("custom_fields_upsellery_transaction_id", StringType),
        Property("custom_fields_upsellery_checkout_id", StringType),
        Property("custom_fields_funnel_slug", StringType),
        Property("custom_fields_plan_id", StringType),
        Property("custom_fields_email", StringType),
        Property("customer_email", StringType),
        Property("customer_first_name", StringType),
        Property("customer_global_id", StringType),
        Property("customer_id", StringType),
        Property("customer_last_name", StringType),
        Property("customer_phone", StringType),
        Property("cvv_response_code", StringType),
        Property("disbursement_details_disbursement_date", StringType),
        Property("disbursement_details_funds_held", BooleanType),
        Property("disbursement_details_settlement_amount", NumberType),
        Property("disbursement_details_settlement_currency_exchange_rate", NumberType),
        Property("disbursement_details_settlement_currency_iso_code", StringType),
        Property("disbursement_details_success", BooleanType),
        # Property("disputes", StringType),
        Property(
            "disputes",
            ArrayType(
                ObjectType(
                    Property("amount", NumberType),
                    Property("amount_disputed", NumberType),
                    Property("amount_won", NumberType),
                    Property("case_number", StringType),
                    Property("chargeback_protection_level", StringType),
                    Property("created_at", StringType),
                    Property("currency_iso_code", StringType),
                    Property("date_opened", StringType),
                    Property("date_won", StringType),
                    Property("global_id", StringType),
                    Property("graphql_id", StringType),
                    Property("id", StringType),
                    Property("kind", StringType),
                    Property("merchant_account_id", StringType),
                    Property("original_dispute_id", StringType),
                    Property("processor_comments", StringType),
                    Property("processor_reply_by_date", StringType),
                    Property("reason", StringType),
                    Property("reason_code", StringType),
                    Property("reason_description", StringType),
                    Property("received_date", StringType),
                    Property("reference_number", StringType),
                    Property("reply_by_date", StringType),
                    Property("response_deadline", StringType),
                    Property("status", StringType),
                    Property("updated_at", DateTimeType),
                    Property(
                        "evidence",
                        ArrayType(
                            ObjectType(
                                Property("id", StringType),
                                Property("tag", StringType),
                                Property("url", StringType),
                                Property("comment", StringType),
                                Property("category", StringType),
                                Property("global_id", StringType),
                                Property("created_at", DateTimeType),
                                Property("graphql_id", StringType),
                                Property("sequence_number", StringType),
                                Property("sent_to_processor_at", StringType),
                            )
                        ),
                    ),
                    Property(
                        "status_history",
                        ArrayType(
                            ObjectType(
                                Property("id", StringType),
                                Property("amount", NumberType),
                                Property("status", StringType),
                                Property("user", StringType),
                                Property("transaction_source", StringType),
                                Property("timestamp", DateTimeType),
                            )
                        ),
                    ),
                )
            ),
        ),
        Property("global_id", StringType),
        Property("graphql_id", StringType),
        Property("merchant_account_id", StringType),
        Property("merchant_address_locality", StringType),
        Property("merchant_address_phone", StringType),
        Property("merchant_address_postal_code", StringType),
        Property("merchant_address_region", StringType),
        Property("merchant_address_street_address", StringType),
        Property("merchant_identification_number", StringType),
        Property("merchant_name", StringType),
        Property("network_response_code", StringType),
        Property("network_response_text", StringType),
        Property("network_transaction_id", StringType),
        Property("order_id", StringType),
        Property("payment_instrument_type", StringType),
        Property("paypal_authorization_id", StringType),
        Property("paypal_billing_agreement_id", StringType),
        Property("paypal_capture_id", StringType),
        Property("paypal_debug_id", StringType),
        Property("paypal_global_id", StringType),
        Property("paypal_image_url", StringType),
        Property("paypal_payer_email", StringType),
        Property("paypal_payer_first_name", StringType),
        Property("paypal_payer_id", StringType),
        Property("paypal_payer_last_name", StringType),
        Property("paypal_payer_status", StringType),
        Property("paypal_payment_id", StringType),
        Property("paypal_refund_from_transaction_fee_amount", StringType),
        Property("paypal_refund_from_transaction_fee_currency_iso_code", StringType),
        Property("paypal_refund_id", StringType),
        Property("paypal_seller_protection_status", StringType),
        Property("paypal_token", StringType),
        Property("paypal_transaction_fee_amount", StringType),
        Property("paypal_transaction_fee_currency_iso_code", StringType),
        Property("pin_verified", BooleanType),
        Property("plan_id", StringType),
        Property("processed_with_network_token", BooleanType),
        Property("processor_authorization_code", StringType),
        Property("processor_response_code", StringType),
        Property("processor_response_text", StringType),
        Property("processor_response_type", StringType),
        Property("processor_settlement_response_code", StringType),
        Property("processor_settlement_response_text", StringType),
        Property("recurring", BooleanType),
        # Property("refund", StringType),
        # Property("refund_ids", StringType),
        # Property("refund_global_ids", StringType),
        Property("refunded_transaction_global_id", StringType),
        Property("refunded_transaction_id", StringType),
        Property("retrieval_reference_number", StringType),
        # Property("risk_data",
        #          ObjectType(
        #              Property("id", StringType),
        #              Property("decision", StringType),
        #              Property("fraud_service_provider", StringType),
        #              Property("device_data_captured", BooleanType),
        #              Property("decision_reasons", StringType)
        #         )
        # ),
        Property("risk_data_id", StringType),
        Property("risk_data_decision_reasons", StringType),
        Property("risk_data_decision", StringType),
        Property("risk_data_device_data_captured", BooleanType),
        Property("risk_data_fraud_service_provider", StringType),
        Property("settlement_batch_id", StringType),
        Property("status", StringType),
        Property(
            "status_history",
            ArrayType(
                ObjectType(
                    Property("id", StringType),
                    Property("amount", NumberType),
                    Property("status", StringType),
                    Property("user", StringType),
                    Property("transaction_source", StringType),
                    Property("timestamp", DateTimeType),
                )
            ),
        ),
        Property("subscription_billing_period_end_date", DateTimeType),
        Property("subscription_billing_period_start_date", DateTimeType),
        Property("subscription_id", StringType),
        Property("tax_exempt", BooleanType),
        Property("terminal_identification_number", StringType),
        Property("type", StringType),
        Property("updated_at", DateTimeType),
        Property("amount_requested", StringType),
        Property("sub_merchant_account_id", StringType),
        Property("master_merchant_account_id", StringType),
        Property("customer_company", StringType),
        Property("customer_website", StringType),
        Property("customer_fax", StringType),
        Property("billing_first_name", StringType),
        Property("billing_last_name", StringType),
        Property("authorized_transaction_id", StringType),
        Property("shipping_id", StringType),
        Property("shipping_first_name", StringType),
        Property("shipping_last_name", StringType),
        Property("shipping_company", StringType),
        Property("shipping_street_address", StringType),
        Property("shipping_extended_address", StringType),
        Property("shipping_locality", StringType),
        Property("shipping_region", StringType),
        Property("shipping_postal_code", StringType),
        Property("shipping_country_name", StringType),
        Property("shipping_country_code_alpha2", StringType),
        Property("shipping_country_code_alpha3", StringType),
        Property("shipping_country_code_numeric", StringType),
        Property("gateway_rejection_reason", StringType),
        Property("purchase_order_number", StringType),
        Property("tax_amount", NumberType),
        Property("surcharge_amount", NumberType),
        Property("credit_card_account_type", StringType),
        Property("credit_card_account_balance", StringType),
        Property("descriptor_name", StringType),
        Property("descriptor_phone", StringType),
        Property("descriptor_url", StringType),
        Property("service_fee_amount", StringType),
        Property(
            "disbursement_details_settlement_base_currency_exchange_rate", StringType
        ),
        Property("three_d_secure_info", StringType),
        Property("ships_from_postal_code", StringType),
        Property("shipping_amount", NumberType),
        Property("shipping_tax_amount", NumberType),
        Property("discount_amount", NumberType),
        Property("retried_transaction_id", StringType),
        Property("retried", BooleanType),
        Property("authorized_transaction_global_id", StringType),
        Property("retried_transaction_global_id", StringType),
        Property("debit_network", StringType),
        Property("processing_mode", StringType),
        # Property("refund_ids", StringType),
        # Property("refund_global_ids", StringType),
        Property("paypal_payee_id", StringType),
        Property("paypal_payee_email", StringType),
        Property("paypal_custom_field", StringType),
        Property("paypal_payer_phone", StringType),
        Property("paypal_description", StringType),
        Property("paypal_shipping_option_id", StringType),
        Property("paypal_cobranded_card_label", StringType),
        Property("paypal_paypal_retail_transaction_id", StringType),
        Property("paypal_paypal_retail_transaction_status", StringType),
        Property("paypal_paypal_retail_transaction_refund_url", StringType),
        Property("paypal_paypal_retail_transaction_lookup_url", StringType),
        Property("paypal_app_used_for_scanning", StringType),
        Property("refunded_installments", StringType),
        Property("payment_receipt_id", StringType),
        Property("payment_receipt_global_id", StringType),
        Property("payment_receipt_amount", StringType),
        Property("payment_receipt_currency_iso_code", StringType),
        Property("payment_receipt_processor_response_code", StringType),
        Property("payment_receipt_processor_response_text", StringType),
        Property("payment_receipt_processor_authorization_code", StringType),
        Property("payment_receipt_merchant_name", StringType),
        Property("payment_receipt_merchant_address_street_address", StringType),
        Property("payment_receipt_merchant_address_locality", StringType),
        Property("payment_receipt_merchant_address_region", StringType),
        Property("payment_receipt_merchant_address_postal_code", StringType),
        Property("payment_receipt_merchant_address_phone", StringType),
        Property("payment_receipt_merchant_identification_number", StringType),
        Property("payment_receipt_terminal_identification_number", StringType),
        Property("payment_receipt_type", StringType),
        Property("payment_receipt_processing_mode", StringType),
        Property("payment_receipt_card_type", StringType),
        Property("payment_receipt_card_last_4", StringType),
        Property("payment_receipt_pin_verified", BooleanType),
        Property(
            "discounts",
            ArrayType(
                ObjectType(
                    Property("amount", NumberType),
                    Property("current_billing_cycle", IntegerType),
                    Property("id", StringType),
                    Property("name", StringType),
                    Property("never_expires", BooleanType),
                    Property("number_of_billing_cycles", IntegerType),
                    Property("quantity", IntegerType),
                )
            ),
        ),
        Property(
            "add_ons",
            ArrayType(
                ObjectType(
                    Property("id", StringType),
                    Property("amount", NumberType),
                    Property("current_billing_cycle", NumberType),
                    Property("name", StringType),
                )
            ),
        ),
        Property("subscription_tax_amount", NumberType),
    ).to_dict()

    def parse_record(self, record: Any) -> dict:
        """Parse the record."""
        parsed = super().parse_record(record)

        # Calculate subscription_tax_amount from add_ons with name "SaaS_TAX".
        if hasattr(record, 'add_ons') and record.add_ons:
            parsed['subscription_tax_amount'] = sum(
                float(add_on.amount)
                for add_on in record.add_ons
                if hasattr(add_on, 'name') and add_on.name == "SaaS_TAX"
            )

        # Calculate discount_amount from discounts object.
        if hasattr(record, 'discounts') and record.discounts:
            parsed['discount_amount'] = sum(
                float(discount.amount)
                for discount in record.discounts
            )

        # If billing country code or billing region is missing try to get it from the
        # customer object.
        if (
            parsed['billing_country_code_alpha2'] is None
            or parsed['billing_region'] is None
            or parsed['billing_country_code_alpha2'] == ''
            or parsed['billing_region'] == ''
        ):
            try:
                customer = braintree.Customer.find(parsed["customer_id"])
                if customer and hasattr(customer, 'addresses') and customer.addresses:
                    # Find first address with a valid country code.
                    valid_address = next(
                        (addr for addr in customer.addresses
                         if hasattr(addr, 'country_code_alpha2') and
                         addr.country_code_alpha2 is not None),
                        None
                    )

                    if valid_address:
                        if not parsed.get('billing_country_code_alpha2'):
                            self.logger.info(f"Found valid address for customer {parsed['customer_id']}")
                            parsed['billing_country_code_alpha2'] = valid_address.country_code_alpha2
                        if not parsed.get('billing_region'):
                            parsed['billing_region'] = valid_address.region
            except braintree.exceptions.NotFoundError:
                self.logger.warning(f"Customer {parsed['customer_id']} not found")

        return parsed


class SubscriptionsStream(BraintreeStream):
    name = "subscriptions"
    primary_keys = ["id"]
    replication_key = None

    braintree_obj = braintree.Subscription
    braintree_search = braintree.SubscriptionSearch.created_at

    schema = PropertiesList(
        Property("id", StringType, required=True),
        Property("balance", NumberType),
        Property("billing_day_of_month", IntegerType),
        Property("billing_period_end_date", StringType),
        Property("billing_period_start_date", StringType),
        Property("created_at", DateTimeType),
        Property("current_billing_cycle", IntegerType),
        Property("days_past_due", IntegerType),
        Property("description", StringType),
        Property("descriptor_name", StringType),
        Property("descriptor_phone", StringType),
        Property("descriptor_url", StringType),
        Property(
            "discounts",
            ArrayType(
                ObjectType(
                    Property("amount", NumberType),
                    Property("current_billing_cycle", IntegerType),
                    Property("id", StringType),
                    Property("name", StringType),
                    Property("never_expires", BooleanType),
                    Property("number_of_billing_cycles", IntegerType),
                    Property("quantity", IntegerType),
                )
            ),
        ),
        Property(
            "disputes",
            ArrayType(
                ObjectType(
                    Property("amount", NumberType),
                    Property("amount_disputed", NumberType),
                    Property("amount_won", NumberType),
                    Property("case_number", StringType),
                    Property("chargeback_protection_level", StringType),
                    Property("created_at", StringType),
                    Property("currency_iso_code", StringType),
                    Property("date_opened", StringType),
                    Property("date_won", StringType),
                    Property("global_id", StringType),
                    Property("graphql_id", StringType),
                    Property("id", StringType),
                    Property("kind", StringType),
                    Property("merchant_account_id", StringType),
                    Property("original_dispute_id", StringType),
                    Property("processor_comments", StringType),
                    Property("processor_reply_by_date", StringType),
                    Property("reason", StringType),
                    Property("reason_code", StringType),
                    Property("reason_description", StringType),
                    Property("received_date", StringType),
                    Property("reference_number", StringType),
                    Property("reply_by_date", StringType),
                    Property("response_deadline", StringType),
                    Property("status", StringType),
                    Property("updated_at", DateTimeType),
                    Property(
                        "evidence",
                        ArrayType(
                            ObjectType(
                                Property("id", StringType),
                                Property("tag", StringType),
                                Property("url", StringType),
                                Property("comment", StringType),
                                Property("category", StringType),
                                Property("global_id", StringType),
                                Property("created_at", DateTimeType),
                                Property("graphql_id", StringType),
                                Property("sequence_number", StringType),
                                Property("sent_to_processor_at", StringType),
                            )
                        ),
                    ),
                    Property(
                        "status_history",
                        ArrayType(
                            ObjectType(
                                Property("id", StringType),
                                Property("amount", NumberType),
                                Property("status", StringType),
                                Property("user", StringType),
                                Property("transaction_source", StringType),
                                Property("timestamp", DateTimeType),
                            )
                        ),
                    ),
                )
            ),
        ),
        Property("failure_count", IntegerType),
        Property("first_billing_date", StringType),
        Property("merchant_account_id", StringType),
        Property("never_expires", BooleanType),
        Property("next_billing_date", StringType),
        Property("next_billing_period_amount", NumberType),
        Property("number_of_billing_cycles", IntegerType),
        Property("paid_through_date", StringType),
        Property("payment_method_token", StringType),
        Property("plan_id", StringType),
        Property("price", NumberType),
        Property("refund_global_ids", StringType),
        Property("refund_ids", StringType),
        Property("status", StringType),
        Property(
            "status_history",
            ArrayType(
                ObjectType(
                    Property("id", StringType),
                    Property("amount", NumberType),
                    Property("status", StringType),
                    Property("user", StringType),
                    Property("subscription_source", StringType),
                    Property("timestamp", DateTimeType),
                )
            ),
        ),
        Property("trial_duration", IntegerType),
        Property("trial_duration_unit", StringType),
        Property("trial_period", BooleanType),
        Property("updated_at", DateTimeType),
    ).to_dict()


class CustomersStream(BraintreeStream):
    name = "customers"
    primary_keys = ["id"]
    replication_method = "INCREMENTAL"
    replication_key = "created_at"

    braintree_obj = braintree.Customer
    braintree_search = braintree.CustomerSearch.created_at

    schema = PropertiesList(
        Property("id", StringType, required=True),
        Property("email", StringType),
        Property("phone", StringType),
        Property("first_name", StringType),
        Property("last_name", StringType),
        Property("created_at", DateTimeType),
        Property("cardholder_name", StringType),
        Property("company", StringType),
        Property("credit_card_expiration_date", StringType),
        Property("credit_card_number", StringType),
        Property("fax", StringType),
        Property("payment_method_token", StringType),
        Property("payment_method_token_with_duplicates", StringType),
        Property("paypal_account_email", StringType),
        Property("website", StringType),
        Property("address_id", StringType),
        Property("address_customer_id", StringType),
        Property("address_first_name", StringType),
        Property("address_last_name", StringType),
        Property("address_company", StringType),
        Property("address_street_address", StringType),
        Property("address_extended_address", StringType),
        Property("address_locality", StringType),
        Property("address_region", StringType),
        Property("address_postal_code", StringType),
        Property("address_country_name", StringType),
        Property("address_country_code_alpha2", StringType),
        Property("address_country_code_alpha3", StringType),
        Property("address_country_code_numeric", StringType),
        Property("address_created_at", DateTimeType),
        Property("address_updated_at", DateTimeType),
    ).to_dict()


class PlansStream(BraintreeStream):
    name = "plans"
    primary_keys = ["id"]
    replication_key = None

    braintree_obj = braintree.Plan
    braintree_search = None

    schema = PropertiesList(
        Property("id", StringType, required=True),
        Property("merchant_id", StringType),
        Property("billing_day_of_month", IntegerType),
        Property("billing_frequency", IntegerType),
        Property("currency_iso_code", StringType),
        Property("description", StringType),
        Property("name", StringType),
        Property("number_of_billing_cycles", IntegerType),
        Property("price", StringType),
        Property("trial_duration", IntegerType),
        Property("trial_duration_unit", StringType),
        Property("trial_period", BooleanType),
        Property("created_at", DateTimeType),
        Property("updated_at", DateTimeType),
        Property("refund_global_ids", StringType),
        Property(
            "discounts",
            ArrayType(
                ObjectType(
                    Property("id", StringType, required=True),
                    Property("amount", NumberType),
                    Property("name", StringType),
                    Property("kind", StringType),
                    Property("description", StringType),
                    Property("created_at", DateTimeType),
                    Property("updated_at", DateTimeType),
                    Property("number_of_billing_cycles", IntegerType),
                    Property("merchant_id", StringType),
                    Property("never_expires", BooleanType),
                )
            ),
        ),
        # Property("refund", StringType),
        # Property("refund_ids", StringType),
        # Property("refund_global_ids", StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        self.logger.info(f" tap_states: {self.tap_state}")

        self.set_braintree_config()
        for record in self.braintree_obj.all():
            yield self.parse_record(record)
