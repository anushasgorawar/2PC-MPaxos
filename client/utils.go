package main

func GetClusterID(client int) int {
	if (client-1)/3000 == 0 {
		return 1
	} else if (client-1)/3000 == 1 {
		return 2
	} else {
		return 3
	}
}
