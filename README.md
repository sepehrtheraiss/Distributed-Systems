# Create a network
docker network create --subnet 192.168.1.1/24 mynet

# First container
docker run -p 8081:8080 --net=mynet --ip=192.168.1.2 -e VIEW="192.168.1.2:8080, 192.168.1.3:8080" -e IP_PORT="192.168.1.2:8080" -e S="1" assignment4

# Second container, and so on...
docker run -p 8082:8080 --net=mynet --ip=192.168.1.3 -e VIEW="192.168.1.2:8080, 192.168.1.3:8080" -e IP_PORT="192.168.1.3:8080" -e S="1" assignment4

# PROTOCOL
views are added and assigned ID's in ascending order