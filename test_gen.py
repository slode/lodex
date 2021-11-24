import uuid


filename = "db_dump.csv"
with open(filename, "w+") as file:
    for i in range(int(1E6)):
        tmp = uuid.uuid4().hex
        file.write("{},{}\n".format(tmp[:20], tmp))

