with open("test.csv", "a") as f:
    i = 1
    while True:
        print(i)
        f.write(f"{i}\n")
        i += 1
