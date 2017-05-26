# Setting up the application
if [ $# -eq 1 ]
  then
    cd $1
else
    echo "Application directory is not specified. Using current directory!"
fi
echo "source venv-messybrainz/bin/activate" > ~/.bashrc
pyvenv-3.4 ../venv-messybrainz
source ../venv-messybrainz/bin/activate
pip3 install -r requirements.txt
python3 manage.py init_db --force
python3 manage.py init_test_db --force
