from datetime import datetime
import os

from base import session
from create_insights import statement
import tables
import xlsxwriter

# Settings
base_path = os.path.abspath(__file__ + "/../../../")
ref_month = datetime.today().strftime("%Y-%m")
ref_day = datetime.today().strftime("%Y-%m-%d")


if __name__ == "__main__":
    # data = session.execute("SELECT * FROM ppr_clean_all").all()
    data = session.execute(statement).all()

    # Create a workbook and add a worksheet.
    workbook = xlsxwriter.Workbook(f"{base_path}/insights_export/{ref_day}_insights.xlsx")

    # Add a new worksheet
    worksheet = workbook.add_worksheet()
    worksheet.set_column(0, 0, 20)

    # Add the table with all results in the newly created worksheet
    worksheet.add_table(
        "A1:F{}".format(len(data) + 1),
        {
            "data": data,
            "columns": [
                {"header": "County"},
                {"header": "Number of Sales 3 month"},
                {"header": "Tot sales 3 months"},
                {"header": "Max sales 3 months"},
                {"header": "Min sales 3 months"},
                {"header": "Avg sales 3 months"},
            ],
        },
    )
    workbook.close()

    print("Data exported to {}".format(f"{base_path}/insights_export/{ref_day}_insights.xlsx"))
