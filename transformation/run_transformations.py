from .cust_info_transformation import transformation_cust_info
from .prd_info_transformation import transformation_prd_info

def run_transformation():

    # Customer info transformation
    transformation_cust_info()
    transformation_prd_info()

if __name__ == "__main__":
    run_transformation()