import argparse
import os

student_number = 1155114481
home_dir = "/home/" + str(student_number) + "/"

servers = ['proj5', 'proj6', 'proj7', 'proj8', 'proj9', 'proj10']

ssh_cmd = (
    "ssh "
    "-o StrictHostKeyChecking=no "
)

# [clone]:
# python3 batch_ops.py -o clone -u https://github.com/RickAi/minips.git
#
# [pull]:
# python3 batch_ops.py -o pull
#
# [build]:
# python3 batch_ops.py -o build -d minips
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Batch Ops Tool')
    parser.add_argument('-o', choices=['clone', 'pull', 'build'], default="pull", help='The operation, e.g. clone, fetch, build')
    parser.add_argument('-u', nargs="?", default=" ", help='The remote repo url the git operation will execute')
    parser.add_argument('-d', nargs="?", default="", help='The dir that will be built')
    args = parser.parse_args()

    op = args.o
    url = args.u
    build_dir = args.d

    if build_dir == "" and op == 'build':
        parser.print_help()
        exit(1)

    for server_name in servers:
        cmd = ssh_cmd + server_name + " "
        cmd += "\""
        cmd += "cd " + home_dir + ";"

        if op == 'build':
            cmd += "cd " + build_dir + ";"
            cmd += "mkdir debug;"
            cmd += "cmake -DCMAKE_BUILD_TYPE=Debug ..;"
        else :
            cmd += "git " + op + " " + url

        cmd += "\" &"
        print(cmd)
        os.system(cmd)
