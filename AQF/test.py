import numpy as np
import pandas as pd
import seaborn as sns; 
#%matplotlib inlinep
import matplotlib.pyplot as plt
a = np.random.randn(30, 2)
a.round(2)
df = pd.DataFrame(a)                          #通过ndarray来构建DataFrame
df.columns = ['Data1', 'Data2']
dates = pd.date_range('2017-1-1', periods = len(a), freq='D')         #时间日期的生成；
df.index = dates
df.head()
#df.plot(figsize= (8, 6), title  = 'Random numbers')
#df['Data2'].plot(figsize = (8,5), ylim = [df['Data2'].min() * 1.2, df['Data2'].max() * 1.2])       #ylim：y轴范围
#df[['Data1','Data2']].plot(subplots=True,figsize = (8, 6));     #子图；
#df.hist(figsize = (9,4), bins = 50)             #直方图；bins：柱的宽度
#df.plot(x = 'Data1',y = 'Data2', kind='scatter',figsize = (8,6), title = 'Scatter plot')         #散点图；画出股价相关性；kind
#df.plot(style=['o', 'b--'],figsize = (8,6))           #颜色和样式都可以个性化修改；
#df.plot(grid=True, figsize=(10, 5),secondary_y = 'Data2', style = '--')          # 解决方案：,secondary_y='Data2'
#df[(df.Data1 > 0.5) & (df.Data2 < -1)].plot(x='Data1', y='Data2', kind='scatter',title = 'Outlier')   # scatter是plot里面的内置参数；


'''
plt.plot([1,2,3,4],[2,4,6,8],lw = 1.0)
plt.ylabel('Numbers')
plt.xlabel('Numbers')


plt.plot([1,2,3,4],[2,4,6,8], 'r--', linewidth=1.0, label = 'Line',)    #'ro',plt.ylim(0,10)
plt.ylabel('Numbers')
plt.legend()     #显示图例一要加的；
plt.ylim(0,10)
'''
t = np.arange(0, 3, 0.2) 
 
plt.plot(t, t, 'r--',
         t, t ** 2, 'b', 
         t, t ** 3, 'm .')

plt.show()