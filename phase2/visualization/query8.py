import matplotlib.pyplot as plt

# Data to plot
labels = ['Non-verified-accounts', 'Verified-Accounts']
sizes = [204260, 1008]
colors = ['yellow', 'red']

# Plot
plt.pie(sizes, labels=labels, colors=colors,
        autopct='%1.1f%%', shadow=True, startangle=140)

plt.axis('equal')
plt.show()