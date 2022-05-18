import matplotlib.pyplot as plt
import pandas as pd

plt.style.use('ggplot')
welsh_datafile = open('C:/Users/bexrk/Documents/OUTPUTS/freqs/_user_soc_myOutput14_freqs_part-r-00000', 'r', encoding='utf-8')
galician_datafile = open('C:/Users/bexrk/Documents/OUTPUTS/freqs/_user_soc_myOutput14_freqs_part-r-00001', 'r', encoding='utf-8' )
norwegian_datafile = open('C:/Users/bexrk/Documents/OUTPUTS/freqs/_user_soc_myOutput14_freqs_part-r-00002', 'r', encoding='utf-8')

def plotwelshdata():
    for data in welsh_datafile:
        (x,y) = data.split(maxsplit=1)
        x_axis = str(x[4:])
        y_axis = float(y)
        plt.bar(x_axis,y_axis, color='r')
        plt.xlabel("Welsh Letters")
        plt.ylabel("Frequency")

def plotgaliciandata():
    for data in galician_datafile:
        (x,y) = data.split(maxsplit=1)
        x_axis = x[4:]
        y_axis = float(y)
        plt.bar(x_axis,y_axis, color='g')
        plt.xlabel("Galician Letters")
        plt.ylabel("Frequency")

def plotnorwegiandata():
    for data in norwegian_datafile:
        (x,y) = data.split(maxsplit=1)
        x_axis = x[4:]
        y_axis = float(y)
        plt.bar(x_axis,y_axis, color='b')
        plt.xlabel("Norwegian Letters")
        plt.ylabel("Frequency")

def plotall():
    welsh_freqs = []
    galician_freqs = []
    norwegian_freqs = []

    for data in welsh_datafile:
        (wx, wy) = data.split(maxsplit=1)
        welsh_freqs.append(float(wy))

    for data in galician_datafile:
        (gx, gy) = data.split(maxsplit=1)
        galician_freqs.append(float(gy))

    for data in norwegian_datafile:
        (nx, ny) = data.split(maxsplit=1)
        norwegian_freqs.append(float(ny))

    plotdata = pd.DataFrame({
        "welsh": welsh_freqs[:26],
        "galician": galician_freqs[:26],
        "norwegian": norwegian_freqs[:26]
    },

        index=["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
               "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]
    )

    plotdata.plot(kind="bar")
    plt.title("Letter Comparisions")
    plt.xlabel("Letters a-z")
    plt.ylabel("Frequencies")

#plotwelshdata()
#plotgaliciandata()
#plotnorwegiandata()
#plotall()
plt.show()
