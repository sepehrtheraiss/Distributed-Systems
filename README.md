# Create a network
docker network create --subnet 192.168.1.1/24 mynet

# First container
docker run -p 8081:8080 --net=mynet --ip=192.168.1.2 -e VIEW="192.168.1.2:8081, 192.168.1.3:8082" -e IP_PORT="192.168.1.2:8081" assignment3

# Second container, and so on...
docker run -p 8082:8080 --net=mynet --ip=192.168.1.3 -e VIEW="192.168.1.2:8081, 192.168.1.3:8082" -e IP_PORT="192.168.1.3:8082" assignment3

# PROTOCOL
views are added and assigned ID's in ascending order

