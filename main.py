with open("data/spddimp.a00", "rb") as f:
    data = f.read()
    print(data[993362632-400:993362632+100])
