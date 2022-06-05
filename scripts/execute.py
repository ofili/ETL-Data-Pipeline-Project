# Import extract, transform and load
import extract, transform, load

# Ensure that execute.py is run from bash
if __name__ == "__main__":
    # Run Extract
    extract.main()
    # Run Transform
    transform.main()
    # Run Load
    load.main()