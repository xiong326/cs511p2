"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-10 21:45:33
 	 FilePath: /codes/pandas_q2.py
 	 Description:
"""
import tempfile

import pandas as pd
import ray
import typing
import util.judge_df_equal
import numpy as np

ray.shutdown()
ray.init()

@ray.remote
def process_chunk(order, lineitem_chunk):
    # ensure column is converted to date
    lineitem_chunk['l_commitdate'] = pd.to_datetime(lineitem_chunk['l_commitdate'])
    lineitem_chunk['l_receiptdate'] = pd.to_datetime(lineitem_chunk['l_receiptdate'])
    
    # filter lineitems before sub-query
    valid_lineitems = lineitem_chunk[lineitem_chunk['l_commitdate'] < lineitem_chunk['l_receiptdate']]

    # sub-query
    valid_orders_keys = valid_lineitems['l_orderkey'].unique()
    valid_orders = order[order['o_orderkey'].isin(valid_orders_keys)]

    # group by and aggregate order count
    result = valid_orders.groupby('o_orderpriority').size().reset_index(name='order_count')

    return result


def ray_q4(time: str, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    #TODO: your codes begin
    start_time = pd.to_datetime(time)
    end_time = start_time + pd.DateOffset(months=3)
    orders['o_orderdate'] = pd.to_datetime(orders['o_orderdate'])

    # filter orders
    filtered_orders = orders[(orders['o_orderdate'] >= start_time) & (orders['o_orderdate'] < end_time)]

    # split line items and start ray tasks
    lineitem_chunks = np.array_split(lineitem, 4)
    futures = [process_chunk.remote(filtered_orders, lineitem_chunk) for lineitem_chunk in lineitem_chunks]
    results = ray.get(futures)

    # concat results
    final_df = pd.concat(results)

    # do a final group by to aggregate results
    final_df = final_df.groupby('o_orderpriority').agg(
        order_count=('order_count', 'sum')
    ).reset_index().sort_values(by='o_orderpriority')

    return final_df
    #end of your codes



if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    orders = pd.read_csv("tables/orders.csv", header=None, delimiter="|")
    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']

    orders.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']

    # run the test
    result = ray_q4("1993-7-01",orders,lineitem)
    # result.to_csv("correct_results/pandas_q4.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q4.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")
