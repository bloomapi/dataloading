package bloomsource

import (
	"fmt"
	"os"
	"io/ioutil"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

func showUsage() {
	fmt.Printf("Usage: %s <command>\n", os.Args[0])
	fmt.Println("=============================\n")
	fmt.Println("Avaialable commands:")
	fmt.Printf("%s bootstrap    # setup datasource in BloomAPI\n", os.Args[0])
	fmt.Printf("%s fetch        # fetch latest data and add to BloomAPI\n", os.Args[0])
	fmt.Printf("%s drop         # remove all tables\n", os.Args[0])
	fmt.Printf("%s search-index # index in elasticsearch\n", os.Args[0])
	fmt.Printf("%s schema       # fetch latest data and scan schema\n", os.Args[0])
}

func CreateCmd(desc Description) {
	if (len(os.Args) != 2) {
		fmt.Println("Invalid command usage\n")
		showUsage()
		os.Exit(1)
	}

	arg := os.Args[1]

	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath("./")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	switch arg {
	case "bootstrap":
		
	case "fetch":
		
	case "drop":
		
	case "search-index":
	case "schema":
		schema, err := schema(desc)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		mapping := SchemaToMapping(schema)
		marshaled, err := yaml.Marshal(&mapping)
		if err != nil {
			fmt.Println("Failed to marshal schema", err)
			os.Exit(1)
		}
		err = ioutil.WriteFile("dbmapping.yaml", marshaled, 0644)
		if err != nil {
			fmt.Println("Failed to write schema", err)
			os.Exit(1)
		}
	default:
		fmt.Println("Invalid command:", arg)
		showUsage()
		os.Exit(1)
	}
}