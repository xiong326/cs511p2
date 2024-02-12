"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-11 10:03:13
 	 FilePath: /codes/pandas_q3.py
 	 Description:
"""
import tempfile

import pandas as pd
import ray
import typing
import numpy as np

import util.judge_df_equal

ray.shutdown()
ray.init()

@ray.remote
def process_chunk(customer, order_chunk, lineitem_chunk):
    # ensure column is converted to date
    order_chunk['o_orderdate'] = pd.to_datetime(order_chunk['o_orderdate'])
    lineitem_chunk['l_shipdate'] = pd.to_datetime(lineitem_chunk['l_shipdate'])

    # filter before join
    filtered_order = order_chunk[order_chunk['o_orderdate'] < pd.to_datetime('1995-03-15')]
    filtered_lineitem = lineitem_chunk[lineitem_chunk['l_shipdate'] > pd.to_datetime('1995-03-15')]
    
    # join
    joined_df = pd.merge(customer, filtered_order, left_on='c_custkey', right_on='o_custkey')
    joined_df = pd.merge(joined_df, filtered_lineitem, left_on='o_orderkey', right_on='l_orderkey')
    
    # group by, calculate revenue, and sort
    result_df = joined_df.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']).agg(
        revenue=('l_extendedprice', lambda x: (x * (1 - joined_df.loc[x.index, 'l_discount'])).sum())
    ).reset_index()
    
    return result_df


def ray_q3(segment: str, customer: pd.DataFrame, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    #TODO: your codes begin
    # filter the customer first
    filtered_customer = customer[customer['c_mktsegment'] == segment]

    # split order and lineitem into chunks
    order_chunks = np.array_split(orders, 2)
    lineitem_chunks = np.array_split(lineitem, 2)

    # compute cross join idx
    cross = []
    for i in range(len(order_chunks)):
        for j in range(len(lineitem_chunks)):
            cross.append((i, j))

    # start ray tasks
    tasks = [process_chunk.remote(filtered_customer, order_chunks[i], lineitem_chunks[j]) for (i, j) in cross]
    results = ray.get(tasks)
    
    # Combine the results
    final_df = pd.concat(results)

    # do another group by to output final results
    final_df = final_df.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']).agg(
        revenue=('revenue', 'sum')
    ).reset_index().sort_values(by=['revenue', 'o_orderdate'], ascending=[False, True]).head(10)

    return final_df
    #end of your codes



if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    orders = pd.read_csv("tables/orders.csv", header=None, delimiter="|")
    customer = pd.read_csv("tables/customer.csv", header=None, delimiter="|")


    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    customer.columns = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
                        'c_comment']
    orders.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']

    # run the test
    result = ray_q3('BUILDING', customer, orders, lineitem)
    # result.to_csv("correct_results/pandas_q3.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q3.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")
