import pandas as pd
import numpy as np
from pandas import DataFrame, Series


def merge_data_set():
    # 合并数据集
    df1 = DataFrame({'key': ['b', 'b', 'a', 'c', 'a', 'a', 'b'], 'data1': range(7)})
    df2 = DataFrame({'key': ['a', 'b', 'd'], 'data2': range(3)})
    print(df1)
    print(df2)
    merge_df1_df2 = pd.merge(df1, df2)  # 如果没有指定要用哪个列进行连接，
                                        # merge就会将重叠列的列名当作键进行连接
    print(merge_df1_df2)

    merge_df1_df2 = pd.merge(df1, df2, on='key')  # 显式指定下列名
    print(merge_df1_df2)
    df3 = DataFrame({'lkey': ['b', 'b', 'a', 'c', 'a', 'a', 'b'], 'data1': range(7)})
    df4 = DataFrame({'rkey': ['a', 'b', 'd'], 'data2': range(3)})
    merge_df3_df4 = pd.merge(df3, df4, left_on='lkey', right_on='rkey')
    print(merge_df3_df4)
    # 默认情况下，merge做的是inner的连接；结果中的键是交集，其他方式还有‘left’，‘right’以及outer。外连接是求键的并集
    outer_merge_df1_df2 = pd.merge(df1, df2, how='outer')
    print('outer_merge_df1_df2:\n',outer_merge_df1_df2)               # 外连接

    df1 = DataFrame({'key': ['b', 'b', 'a', 'c', 'a', 'b'], 'data1': range(6)}) 
    df2 = DataFrame({'key': ['a', 'b', 'a', 'b', 'd'], 'data2': range(5)})
    
    left_merge_df1_df2 = pd.merge(df1, df2, on='key', how='left')        # 多对多的连接,产生的是行的笛卡尔积，由于左边有三个b行，右边有两个，所以最终结果是有6个b行
    print('left_merge_df1_df2:\n' ,left_merge_df1_df2)
    # 如果要根据多个键进行合并操作，传入一个由列名组成的列表即可：
    left = DataFrame({'key1':['foo', 'foo', 'bar'],
                      'key2':['one', 'two', 'one'],
                      'lval':[1, 2, 3]})
    right = DataFrame({'key1':['foo', 'foo', 'bar', 'bar'],
                      'key2':['one', 'one', 'one', 'two'],
                      'rval':[4, 5, 6, 7]}
                      )
    more_key_merge_df1_df2 = pd.merge(left, right, on=['key1', 'key2'],how='outer')
    print('more_key_merge_df1_df2:\n', more_key_merge_df1_df2)        # 结果中会出现哪些键组合取决于所选的合并方式，你可以这样来理解：多个键形成一系列元祖
                                                                      # 并将其当作单个连接键，实际上却并不是这样子的
    norm_key_merge_df1_df2 = pd.merge(left, right, on='key1')
    print('norm_key_merge_df1_df2:\n', norm_key_merge_df1_df2)      # 正常的数据集merge

    suf_key_merge_df1_df2 = pd.merge(left, right, on='key1', suffixes=('_left', '_right'))
    print('suf_key_merge_df1_df2:\n', suf_key_merge_df1_df2)    # suffixes选项，用于指定附加到左右两个dataframe对象的重叠列名上的字符串
    """
     merge的参数包括：left 参与合并的左侧DataFrame
                    right,参与合并的右侧DataFrame
    how,             inner，outer，left，right
    on,              用于连接的列名    
    left_on,          左侧Dataframe中用作连接的键
    right_on,         右侧Dataframe中用作连接的键
    left_index,        将左侧的行索引用作其连接键
    right_index,
    sort,               根据连接键对合并后的数据进行排序，默认为True
    suffixes,           字符串值元组，用于追加到重叠列名的末尾
    copy                设置为False 的话可以在某些特殊情况下避免将数据复制到结果数据结构中
    """
    # 索引上的合并
    # 有时候Dataframe中的连接键位于其索引中，传入left_index以说明索引应该被用作连接键：
    left1 = DataFrame({'key':['a','b','a','a','b','c'], 'value':range(6)})
    right1 = DataFrame({'group_val': [3.5, 7]}, index=['a','b'])
    print('left1 :\n',left1 , '\nright1 :\n',right1)
    
    index_merge_left1_right1 = pd.merge(left1, right1, left_on='key', right_index=True)
    print('index_merge_left1_right1:\n', index_merge_left1_right1)   # 按照索引来合并

    lefth = DataFrame({'key1': ['Ohio', 'Ohio', 'Ohio', 'Nevada' , 'Nevada' ],
                       'key2': [2000, 2001, 2002, 2001, 2002],
                       'data': np.arange(5.0)})
    righth = DataFrame(np.arange(12).reshape((6,2)),index=[['Nevada', 'Nevada', 'Ohio', 'Ohio', 'Ohio', 'Ohio'],
                                                               [2001, 2000, 2000, 2000, 2001, 2002]],
                                                            columns=['event1', 'event2'])     # 带层次化索引的数据
    print('lefth:\n' ,lefth)
    print('righth:\n' , righth)

    
    merge_lefth_righth = pd.merge(lefth, righth, left_on=['key1','key2'], right_index=True)   # 带层次化索引的数据必须以列表的形式指明用作合并键的多个列
    print('merge_lefth_righth:\n',merge_lefth_righth)

    outer_merge_lefth_righth = pd.merge(lefth, righth, left_on=['key1','key2'], right_index=True, how='outer')
    print('outer_merge_lefth_righth:\n', outer_merge_lefth_righth)          # 外连接

    left2 = DataFrame([[1.,2. ], [3., 4.],[5., 6.]],index=['a','c','e'], columns=['Ohio', 'Nevada'])
    print(left2)

    right2 = DataFrame([[7., 8.], [9., 10.], [11., 12.], [13, 14]],index=['b','c','d','e'],columns=['Missouri','Alabama'])
    print(right2)
    
    outer_merge_left2_right2 = pd.merge(left2, right2, how='outer', left_index=True, right_index=True)
    print(outer_merge_left2_right2)
    # pd.merge(lefth, righth, left_on=['key1','key2'], right_index=True, how='outer')
    # join方法也可以实现按索引合并
    join_left2_right2 = left2.join(right2, how='outer')   # 它能更方便地实现按索引合并。它还可用于合并多个带有相同或相似索引的DataFrame对象
    print(join_left2_right2)

    left1 = DataFrame({'key':['a','b','a','a','b','c'], 'value':range(6)})
    right1 = DataFrame({'group_val': [3.5, 7]}, index=['a','b'])
    print(left1, right1)

    
def main():
    merge_data_set()


if __name__ == '__main__':
    main()
