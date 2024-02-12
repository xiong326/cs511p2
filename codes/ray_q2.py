"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-10 21:45:33
 	 FilePath: /codes/pandas_q2.py
 	 Description:
"""
import pandas as pd
import ray
import typing
import util.judge_df_equal
import tempfile
import numpy as np

# Initialize Ray
ray.shutdown()
ray.init()

@ray.remote
def process_chunk(chunk, timediff):
    # ensure column is converted to date
    chunk['l_shipdate'] = pd.to_datetime(chunk['l_shipdate'])
    last_day = pd.to_datetime('1998-12-01') - pd.DateOffset(days=timediff)

    # filter the DataFrame
    filtered_df = chunk[chunk['l_shipdate'] <= last_day]
    
    # group by l_returnflag and l_linestatus and calculate the aggregates
    result_df = filtered_df.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=('l_quantity', 'sum'),
        sum_base_price=('l_extendedprice', 'sum'),
        sum_disc_price=('l_extendedprice', lambda x: (x * (1 - filtered_df.loc[x.index, 'l_discount'])).sum()),
        sum_charge=('l_extendedprice', lambda x: (x * (1 - filtered_df.loc[x.index, 'l_discount']) * (1 + filtered_df.loc[x.index, 'l_tax'])).sum()),
        sum_disc=('l_discount', 'sum'),
        count_order=('l_orderkey', 'count')
    ).reset_index()
    
    return result_df


def ray_q2(timediff:int, lineitem:pd.DataFrame) -> pd.DataFrame:
    #TODO: your codes begin
    chunks = np.array_split(lineitem, ray.available_resources()['CPU'])
    
    # process each chunk in parallel
    tasks = [process_chunk.remote(chunk, timediff) for chunk in chunks]
    results = ray.get(tasks)
    
    # combine the results
    final_df = pd.concat(results)
    
    # further group by and aggregate to combine results from chunks correctly
    final_result_df = final_df.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=('sum_qty', 'sum'),
        sum_base_price=('sum_base_price', 'sum'),
        sum_disc_price=('sum_disc_price', 'sum'),
        sum_charge=('sum_charge', 'sum'),
        avg_qty=('sum_qty', 'sum'),
        avg_price=('sum_base_price', 'sum'),
        avg_disc=('sum_disc', 'sum'),
        count_order=('count_order', 'sum')
    ).reset_index()

    final_result_df['avg_qty'] = final_result_df['avg_qty'] / final_result_df['count_order']
    final_result_df['avg_price'] = final_result_df['avg_price'] / final_result_df['count_order']
    final_result_df['avg_disc'] = final_result_df['avg_disc'] / final_result_df['count_order']
    
    # order by l_returnflag and l_linestatus
    final_result_df = final_result_df.sort_values(by=['l_returnflag', 'l_linestatus'])
    
    return final_result_df
    #end of your codes



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
    result = ray_q2(90, lineitem)
    # result.to_csv("correct_results/pandas_q2.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q2.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")


