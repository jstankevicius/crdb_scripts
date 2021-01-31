from control import ssh_cmd
import os
N_NODES = 4
ROACHDIR = "~/go/src/github.com/cockroachdb/cockroach"

def main():
    # move binary from build directory into /usr/local/bin, here and on
    # all the other nodes.
    #os.system("sudo cp {}/cockroach /usr/local/bin".format(ROACHDIR))

    for i in range(N_NODES):
        os.system("sudo scp {}/cockroach root@node-{}:/usr/local/bin".format(ROACHDIR, str(i)))

if __name__ == "__main__":
    main()