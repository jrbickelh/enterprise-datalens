import great_expectations as gx

def setup_quality_suite():
    context = gx.get_context()
    
    # 1. Define the Suite
    suite_name = "banking_data_suite"
    suite = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

    # 2. Add Expectations for your Banking Data
    # These match the columns you generated in build_lakehouse.py
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="transaction_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="status", 
        value_set=["Active", "Inactive"]
    ))
    
    return context, suite_name

def get_documented_suite(context):
    suite_name = "banking_quality_suite"
    
    # Updated API for GX 1.0+
    suite = context.suites.add_or_update(gx.ExpectationSuite(name=suite_name))

    column_descriptions = {
        "transaction_id": "Unique identifier for each banking transaction. Primary Key.",
        "customer_id": "Unique identifier for the customer. Foreign Key link to CRM.",
        "amount": "The monetary value of the transaction in USD. Must be positive.",
        "status": "Current state of the customer account (Active/Inactive).",
        "timestamp": "ISO-8601 formatted time of transaction execution."
    }

    # Apply expectations using the new suite.add_expectation method
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
        column="transaction_id",
        notes=column_descriptions["transaction_id"]
    ))
    
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="amount", 
        min_value=0,
        notes=column_descriptions["amount"]
    ))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="status", 
        value_set=["Active", "Inactive"],
        notes=column_descriptions["status"]
    ))

    return suite
