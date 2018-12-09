
import matplotlib.pyplot as plt
import numpy as np
labels = ['Amazon', 'Google', 'Walmart', 'Apple', 'Yahoo', 'At&t', 'oracle', 'Tesla', 'Intel', 'Uber', 'Sony']
sizes = [655, 213, 30, 246, 64,0, 11, 20, 12, 35, 45]

def bar_graph():
    index = np.arange(len(labels))
    plt.bar(index, sizes, width=.2)
    plt.xlabel('Brand')
    plt.ylabel('Count')
    plt.xticks(index, labels)
    plt.title('Number of Tweets on different types of brands')
    plt.show()

bar_graph()
