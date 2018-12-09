
import matplotlib.pyplot as plt
import numpy as np
labels = ['Tynisha taylor', 'wwu radio', '.', 'Magnet Leisure', 'Deepak Sharma', 'miyagi doesnt ce...', 'Carol', 'Pyccknx...', 'Akash']
sizes = [2,16,11,34,3,3,3,4,6]

def bar_graph():
    index = np.arange(len(labels))
    plt.bar(index, sizes)
    plt.xlabel('Name')
    plt.ylabel('Count')
    plt.xticks(index, labels)
    plt.title('Max number of tweets about particular brand')
    plt.show()

bar_graph()
