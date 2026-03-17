from .cust_info_transformation import transformation_cust_info
from .prd_info_transformation import transformation_prd_info
from .sales_details_transformation import transformation_sales_details
from .cust_az12_transformation import transformation_cust_az12
from .loc_a101_transformation import transformation_loc_a101
from .px_cat_g1v2_transformation import transformation_px_cat_g1v2

def run_transformation():

    # Customer info transformation
    transformation_cust_info()
    transformation_prd_info()
    transformation_sales_details()
    transformation_cust_az12()
    transformation_loc_a101()
    transformation_px_cat_g1v2()

if __name__ == "__main__":
    run_transformation()