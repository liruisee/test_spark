from random import randint


f = open("C:/Users/lirui/Desktop/test.txt", 'w')
tem_str = 'abcd'

for i in range(5000):
    wr_str = ''
    _count = randint(1,3)
    for j in range(_count):
        index = randint(0,3)
        wr_str += tem_str[index]
    f.write(wr_str+'\n')
f.close()