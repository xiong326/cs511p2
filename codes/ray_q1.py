"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:47
 	 LastEditTime: 2023-09-11 10:37:24
 	 FilePath: /codes/pandas_q1.py
 	 Description:
"""
import pandas as pd
import ray
import typing
import numpy as np

ray.shutdown()
ray.init()

@ray.remote
def process_chunk(chunk):
    # Convert shipdate to datetime format within the chunk
    chunk['l_shipdate'] = pd.to_datetime(chunk['l_shipdate'])
    
    # filter the chunk based on the conditions
    filtered_chunk = chunk[
        (chunk['l_shipdate'] >= start_date) &
        (chunk['l_shipdate'] < end_date) &
        (chunk['l_discount'] >= 0.05) & 
        (chunk['l_discount'] <= 0.070001) &
        (chunk['l_quantity'] < 24)
    ]
    
    # calculate and return the revenue for this chunk
    return (filtered_chunk['l_extendedprice'] * filtered_chunk['l_discount']).sum()


def ray_q1(time: str, lineitem:pd.DataFrame) -> float:
    # TODO: your codes begin
    global start_date, end_date
    start_date = pd.to_datetime(time, format='%Y-%m-%d')
    end_date = start_date + pd.DateOffset(years=1)
    
    # split the DataFrame into chunks
    chunks = np.array_split(lineitem, 4)

    # process chunks in parallel
    tasks = [process_chunk.remote(chunk) for chunk in chunks]
    
    # gather and sum up the results
    revenue = sum(ray.get(tasks))
    
    return revenue
    # end of your codes



if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    # run the test
    result = ray_q1("1994-01-01", lineitem)
    try:
        assert abs(result - 123141078.2283) < 0.01
        print("*******************pass**********************")
    except Exception as e:
        logger.error("Exception Occurred:" + str(e))
        print(f"*******************failed, your incorrect result is {result}**************")
