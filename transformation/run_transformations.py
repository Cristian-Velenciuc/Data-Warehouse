from .cust_info_transformation import transformation_cust_info
from .prd_info_transformation import transformation_prd_info
from .sales_details_transformation import transformation_sales_details


def run_transformation():

    # Customer info transformation
    transformation_cust_info()
    transformation_prd_info()
    transformation_sales_details()

if __name__ == "__main__":
    run_transformation()