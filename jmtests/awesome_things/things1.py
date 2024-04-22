from jobmaster import task, same, Dependency


@task
def foo(file_path: str, number: [1, 2, 3]):
    with open(file_path, 'w') as f:
        f.write(str(number))


@task(dependencies=Dependency(foo, 1, file_path=same))
def bar(file_path: str, number: [4, 5, 6]):
    with open(file_path, 'r') as f:
        old_number = int(f.read())
    with open(file_path, 'w') as f:
        f.write(str(old_number*number))
