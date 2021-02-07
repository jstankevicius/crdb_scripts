sudo usermod -aG docker $USER
sudo apt-get install -y python3-pip
pip3 install -r requirements.txt
cd ~
mkdir go
sudo mount /dev/sda4 go
sudo chmod -R 777 go
mkdir -p go/src/github.com/cockroachdb
cd go/src/github.com/cockroachdb
git config --global user.name "jstankevicius"
git config --global user.email "juuuuuustas@gmail.com"
git clone https://github.com/jstankevicius/cockroach
