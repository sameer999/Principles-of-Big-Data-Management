import matplotlib.pyplot as plt
import numpy as np
label=['Fri Dec 07', 'null']
tweet_count=[205735, 1941]

colors = ['orange', 'seagreen']

fig1, ax1 = plt.subplots()
ax1.pie(tweet_count, colors=colors, labels=label, explode=(0.0,0.0), autopct='%1.1f%%', startangle=90)
# draw circle
centre_circle = plt.Circle((0, 0), fc='white')
fig = plt.gcf()
fig.gca().add_artist(centre_circle)
# Equal aspect ratio ensures that pie is drawn as a circle
ax1.axis('equal')
plt.tight_layout()
plt.title('Tweets activity based on Date')
plt.show()