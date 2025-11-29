package main

func GetClusterID(client int) int {
	switch (client - 1) / 3000 {
	case 0:
		return 1
	case 1:
		return 2
	default:
		return 3
	}

}
