class A:
    def __init__(self):
        print('init')
        self.a = 0

    def __enter__(self):
        print('enter')
        self.a += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('exit')
        self.a -= 1

with A() as aa:
    print(aa)


