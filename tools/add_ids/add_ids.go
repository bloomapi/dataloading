package main

import (
	"log"
	"os"
	"github.com/spf13/viper"
	"github.com/gocodo/bloomdb"
)

func main() {
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath("./")

	configPath := os.Getenv("BLOOM_CONFIG")
	if configPath != "" {
		viper.AddConfigPath(configPath)
	}

	err := viper.ReadInConfig()
	if err != nil {
		log.Fatal(err)
	}

	bdb := bloomdb.CreateDB()
	conn, err := bdb.SqlConnection()
	if err != nil {
		log.Fatal(err)
	}

	rows, err := conn.Query("SELECT name FROM search_types");
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			log.Fatal(err)
		}

		key := bloomdb.MakeKey(name)
		_, err = conn.Exec("UPDATE search_types SET id = $1 WHERE name = $2", key, name)
	}

	log.Println("Done!")
}