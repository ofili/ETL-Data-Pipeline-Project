from common.base import session
from common.tables import PprRawAll, PprCleanAll

from sqlalchemy import cast, Integer, Date
from sqlalchemy.dialects.postgresql import insert

def insert_transactions():
    """
    Insert operation: add new data
    """

    # Retrieve all transactions ids from the clean table
    clean_transaction_ids = session.query(PprCleanAll.transaction_id)

    # date_of_sale and price columns are casted to their respective
    # datatypes 
    transactions_to_insert = session.query(
        cast(PprRawAll.date_of_sale, Date),
        PprRawAll.address,
        PprRawAll.postal_code,
        PprRawAll.county,
        cast(PprRawAll.price, Integer),
        PprRawAll.description,
    ).filter(~PprRawAll.transaction_id.in_(clean_transaction_ids)) 

    # Print the number of transactions to insert
    print("Transactions to insert:", transactions_to_insert.count())

    # Insert the rows from the previously selected transactions
    # into the clean table
    stmt = insert(PprCleanAll).from_select(
        ["date_of_sale", "address", "postal_code", "county", "price", "description"], transactions_to_insert,)
    
    # Execute and commit the statement to make the changes permanent
    session.execute(stmt)
    session.commit()


def delete_transactions():
    """
    Delete operation: delete any row not present in the last snapshot
    """

    # Retrieve all transactions ids from the raw table
    raw_transactions_ids = session.query(PprRawAll.transaction_id) #.all()

    # filter all the ppr_clean_all table transactions that are not present in the ppr_raw_all table
    # and delete them
    # Passing sychronize_session=False to the delete() method tells SQLAlchemy to not wait for the session to be
    # flushed before deleting the row.
    transactions_to_delete = session.query(PprCleanAll).filter(~PprCleanAll.transaction_id.in_(raw_transactions_ids))

    # Print the number of transactions to delete
    print(f"Number of transactions to delete: {transactions_to_delete.count()}")

    # Delete the transactions
    transactions_to_delete.delete(synchronize_session=False)

    # Commit the changes to the database
    session.commit()


def main():
    """
    Main function
    """
    print("[Load] Starting load operation")
    print("[Load] Inserting new rows")
    # Insert transactions
    insert_transactions()

    print("[Load] Deleting rows not available in the transformed data")
    # Delete transactions
    delete_transactions()
    print("[Load] Finished load operation")

    print("[Load] End")