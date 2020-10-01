import csv
import matplotlib.pyplot as plt

messages = []
threads = []

with open('num_of_messages.csv', newline='') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    n = 0
    for row in csvreader:
        n += 1
        messages.append(row[0])
        try:
            threads.append(row[1])
        except:
            threads.append(0)

    x_axis = range(n)
    plt.plot(threads,'-,k',color="blue")
    #plt.plot(messages,'+',color="red")


    plt.show()
