import pandas as pd
import numpy as np
import os,binascii
import sys
from multiprocessing.pool import ThreadPool
def __generate(row,index):
    col=int(1000/4)
    def random_nan(col_num,nan_num):
        for i in range(col_num):
            col_index = int(np.random.uniform(0,len(all_df.columns)))
            all_df[col_index][all_df[col_index].sample(nan_num).index]=np.nan
    def random_str(row,col):
        data = np.empty([row,col],dtype=object)
        for i in range(row):
            for j in range(col):
                data[i][j] = binascii.b2a_hex(os.urandom(5))
        return data
    
    def task1():
        float_part =pd.DataFrame(np.random.randn(row,col),columns=range(0,col))
        return float_part
    def task2():
        int_part = pd.DataFrame(np.random.randint(1,1000000,[row,col]),columns=range(col,2*col))
        return int_part
    def task3():
        string_part = pd.DataFrame(random_str(row,col),columns=range(2*col,3*col))
        return string_part
    def task4():
        binary_part = pd.DataFrame(np.random.randint(0,2,[row,col]),columns=range(3*col,4*col))
        return binary_part
    pool = ThreadPool(processes=4)
    async_result = pool.apply_async(task1)
    float_part = async_result.get()
    async_result = pool.apply_async(task2)
    int_part = async_result.get()
    async_result = pool.apply_async(task3)
    string_part = async_result.get()
    async_result = pool.apply_async(task4)
    binary_part = async_result.get()
    all_df = pd.concat([float_part,int_part,string_part,binary_part],axis=1)
    
    random_nan(col,int(row/10))
    all_df.to_csv("{}.csv".format(index),index=False)



def generate(single_file_row, file_num):
    for i in range(file_num):
        __generate(single_file_row, i)


if __name__=="__main__":
    single_file_row = int(sys.argv[1])
    file_num = int(sys.argv[2])
    generate(single_file_row, file_num)