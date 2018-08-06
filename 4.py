import pandas as pd
import datetime
from pandas_datareader import data, wb
import numpy as np
import time


start_time = time.time()

msft= pd.read_csv("/home/siddhesh/Documents/projects/pandas/input.csv",skipinitialspace=True)



print("Adults")
print("---------------------------")
print(msft[msft['Age']>18].count() - msft[msft['Age']>58].count()  )
                                             
print("---------------------------")


print("Over-aged")
print("---------------------------")
print(msft[msft['Age']>58].count())
print("---------------------------")






print("Teenagers")
print("---------------------------")
print(msft[msft['Age']>12].count() - msft[msft['Age']>19].count()  )
                                             
print("---------------------------")



print("Kids")
print("---------------------------")
print(msft[msft['Age']>0].count() - msft[msft['Age']>12].count()  )
                                             
print("---------------------------")

print("--- %s seconds ---" % (time.time() - start_time))

