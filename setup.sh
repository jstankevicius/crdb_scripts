sudo usermod -aG docker $USER
newgrp docker
exit
echo "modified docker perms"
sudo apt-get install python3-pip
pip3 install -r requirements.txt
cd ~
mkdir go
sudo mount /dev/sda4 go
sudo chmod -R 777 go
mkdir -p go/src/github.com/cockroachdb
echo "created go build path"
cd go/src/github.com/cockroachdb 
git clone https://github.com/jstankevicius/cockroach
