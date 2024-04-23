from jobmaster import task, same, Dependency


@task
def foo(file_path: str, number: [1, 2, 3]):
    """
    Write a number to a file.

    :param file_path: the file to write to
    :param number: the number to write
    """
    with open(file_path, 'w') as f:
        f.write(str(number))


@task(dependencies=Dependency(foo, 1, file_path=same))
def bar(file_path: str, number: [4, 5, 6]):
    """
    Multiply the number in a file by another number.

    :param file_path: file_path: the file to read from and write to
    :param number: the number to multiply by
    :return:
    """
    with open(file_path, 'r') as f:
        old_number = int(f.read())
    with open(file_path, 'w') as f:
        f.write(str(old_number*number))
