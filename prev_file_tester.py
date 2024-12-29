f = r'msc/test_file_MARCH_15 (1).xlsx'
import pandas as pd
import numpy as np
df = pd.read_excel(f)
df.fillna(0,inplace=True)
df['LTP'] = df['LTP'].apply(lambda x: int(x))

#for plotting puposes
red_line_points,green_line_points = [],[]
red_notice = True
#variables:
slice_len =400
decision_range = 20
error_range =.3


import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
plt.ion()
plt.figure(figsize=(20, 10),dpi=400) 
plt.xticks(fontsize=4)
graf = False
polariser = {} # {LTP: buyquantity}
total_buy_qty = 0
long= []
short = []
mean,std = [],[]
for x in df.rolling(1):
    current_ltp = x['LTP'].values[0]

    if current_ltp in polariser.keys():

        polariser[current_ltp] +=x['Buy Qty'].item()
    else:
        polariser[current_ltp] = x['Buy Qty'].item()


    total_buy_qty+=x['Buy Qty'].item()
    count = len([x for x in polariser.keys() if polariser[x]>0])
    avg_qty = total_buy_qty/count if count>0 else 0# average updates each time we get a new update. 
    keys = sorted(polariser.keys())
    traded_time = x['Date Time'].values[0]

    if polariser[current_ltp]<=avg_qty:
        red_line_points.append(x['LTP'].item())
        green_line_points.append(np.nan)
        if len(green_line_points)>0 and not red_notice:
            green_line_points[-1] = x['LTP'].item()
        red_notice = True
    else:
        print(x['Buy Qty'].item())
        green_line_points.append(x['LTP'].item())
        red_line_points.append(np.nan)
        if len(red_line_points)>0 and red_notice :
            red_line_points[-1] = x['LTP'].item()
        red_notice= False


    below, above  = keys[:keys.index(current_ltp)],keys[keys.index(current_ltp):]

    print(f"\n\n\n{current_ltp} {x.index[-1]},::{traded_time}:: {len(keys)}, len below, above: {len(below)}, {len(above)}, {total_buy_qty}, {count}, {avg_qty} \below:{[polariser[k] for k in below]}, above:{[polariser[k] for k in above]}")

    # min and max calculations
    if x.index[-1]>=slice_len:
        slice = df["LTP"][x.index[-1]+1-slice_len:x.index[-1]+1].sort_values()
    else:
        slice = df["LTP"][:x.index[-1]+1].sort_values()
    maxes = slice[-3:].to_list()
    mins = slice[:3].to_list()
    print(mins,maxes)
    # mean and std calculations
    if x.index[-1]>=slice_len//2:
        mean.append( df["LTP"][x.index[-1]+1-slice_len//2:x.index[-1]+1].mean() )
        std.append( df["LTP"][x.index[-1]+1-slice_len//2:x.index[-1]+1].std() )
    else:
        mean.append(np.nan)
        std.append(np.nan)


    # potential short signal : len(above)<20 and most of them are 0s and below>20 and most of them are reds
    if len(below)>decision_range:
        print('\tchecking for a short')
        greens,reds = 0,0
        for key in below[:-decision_range:-1]:
            if polariser[key]<=avg_qty:
                reds+=1
            else:
                greens+=1
        print(f'\treds:{reds},greens:{greens}')


        if reds>=decision_range*(1-error_range) and greens/(reds+greens)<=error_range and current_ltp in maxes:
            print(f'SHORT SIGNAL:\n\ttraded_time:{x.index[-1]}: 20+gap found at LTP: {current_ltp},reds:{reds},greens:{greens}\n')
            short.append(x.index[-1])
        


    # potential buy signal : short but inverted
    if len(above)>decision_range:
        print('\tchecking for a buy')
        greens,reds = 0,0
        for key in above[:decision_range]:
            if polariser[key]<=avg_qty:
                reds+=1
            else:
                greens+=1
        print(f'\treds:{reds},greens:{greens}')
        if reds>=decision_range*(1-error_range) and greens/(reds+greens)<=error_range and current_ltp in mins:
            print(f'BUY SIGNAL:\n\ttraded_time:{x.index[-1]} 20+gap found at LTP: {current_ltp},reds:{reds},greens:{greens}\n')
            long.append(x.index[-1])
    if graf:
        ax.remove()
    ax = pd.Series(red_line_points).plot(color='#B22222',linewidth=1)
    ax.plot(pd.Series(green_line_points),color='#228B22',linewidth=1)
    ax.xaxis.set_major_locator(MultipleLocator(100))
    ax.yaxis.set_major_locator(MultipleLocator(10))
    graf=True
    print(short)
    print(long)

    for point in short:
        ax.plot( point,df['LTP'][point], 'ro', markersize=5)

    for point in long:
        ax.plot( point,df['LTP'][point], 'bo', markersize=5)
    ax.plot(mean,linewidth=1)
    #ax.plot([mean[x]+2*std[x] for x in range(len(mean))],linewidth=1)
    #ax.plot([mean[x]-2*std[x] for x in range(len(mean))],linewidth=1)
    ax.plot(pd.Series(std)+min(polariser.keys())-50,linewidth=1)

    plt.grid()
    plt.title(f)
    plt.pause(0.025)
#(df['vol'].rolling(400).std()+6790).plot()

"""
ema100 = df['LTP'].ewm(span=100).mean()
rolling_std = df['vol'].rolling(100).std()
bolup1 = ema100+2*rolling_std
boldown1 = ema100-2*rolling_std
ax.plot(ema100,linewidth=1)
ax.plot(bolup1,linewidth=1)
ax.plot(boldown1,linewidth=1)
ax.plot(rolling_std+6790)
"""

