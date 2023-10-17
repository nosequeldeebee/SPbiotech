package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"strings"

	"github.com/davecgh/go-spew/spew"
	_ "github.com/mattn/go-sqlite3"
)

type Company struct {
	Name                   string
	Employees              string
	FoundingYear           string
	Industry               string
	NumberOfInvestors      int
	Revenue                string
	NetIncome              string
	CompanyType            string
	NumberOfPriorInvestors int
	NumberOfCompetitors    int
	NumberOfAlliances      int
}

func main() {

	// initiatilize the database
	db, err := sql.Open("sqlite3", "./companies.db")
	if err != nil {
		log.Fatal("Error opening the database:", err)
	}
	defer db.Close()

	var companies []string

	// get a list of all the companies in the database
	rows, err := db.Query("SELECT name FROM companies ORDER BY name COLLATE NOCASE ASC")
	if err != nil {
		log.Fatal("Error querying the database:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var company string
		err := rows.Scan(&company)
		if err != nil {
			log.Fatal(err)
		}
		companies = append(companies, company)
	}

	// for each company, extract the data from the file and write it to the database
	for _, company := range companies {
		var c Company
		c, err := extractDataFromCompanyFile(company)
		if err != nil {
			fmt.Println(err)
			continue
		}
		c.Name = company

		// this is just a pretty printer for debugging. We can delete this at the end.
		spew.Dump(c)

		if err := writeCompanyToDatabase(db, c); err != nil {
			fmt.Println(err)
			continue
		}

		fmt.Println("successfully completed company: ", company)
	}

}

// extractDataFromCompanyFile extracts the data from the file for a given company based on regular expressions for each field of interest
func extractDataFromCompanyFile(company string) (Company, error) {
	var c Company
	fileName := "./S&P/" + company + ".txt"
	fileContent, err := ioutil.ReadFile(fileName)
	if err != nil {
		return c, err
	}

	// extract data that has common regex patterns across files
	yearPattern := `Year Founded: (\d{4})`
	regexpPattern := regexp.MustCompile(yearPattern)
	matches := regexpPattern.FindStringSubmatch(string(fileContent))
	if len(matches) != 0 {
		c.FoundingYear = matches[1]
	}

	industryPattern := `Primary Industry Classification\n([^\n]+)`
	regexpPattern = regexp.MustCompile(industryPattern)
	matches = regexpPattern.FindStringSubmatch(string(fileContent))
	if len(matches) != 0 {
		c.Industry = matches[1]
	}

	revenuePattern := `Total Revenue\n([^\n]+)`
	regexpPattern = regexp.MustCompile(revenuePattern)
	matches = regexpPattern.FindStringSubmatch(string(fileContent))
	if len(matches) != 0 {
		if matches[1] != "-" {
			c.Revenue = matches[1]
		}
	}

	netIncomePattern := `Net Income\n([^\n]+)`
	regexpPattern = regexp.MustCompile(netIncomePattern)
	matches = regexpPattern.FindStringSubmatch(string(fileContent))
	if len(matches) != 0 {
		// sometimes if data is not available in the file it is represented as a dash.
		// we don't want the dash in the database so we check for it and only add the data if it is not a dash
		if matches[1] != "-" {
			c.NetIncome = matches[1]
		}
	}

	// check for the presence of 'Ticker:' which implies a public company
	publicPattern := `Ticker:`
	regexpPattern = regexp.MustCompile(publicPattern)
	match := regexpPattern.FindString(string(fileContent))

	// follow two different paths of data extraction depending on the company type (public vs. private)
	// because they have different content formats in the files for certain data
	switch {
	case match != "":
		c.CompanyType = "Public"

		employeesPattern := `Number of Employees: (\d+)`
		regexpPattern = regexp.MustCompile(employeesPattern)
		matches := regexpPattern.FindStringSubmatch(string(fileContent))
		if len(matches) != 0 {
			c.Employees = matches[1]
		}

		numberOfInvestorsPattern := `Current and Pending Investors\n([\s\S]+?)\n\n`
		regexpPattern = regexp.MustCompile(numberOfInvestorsPattern)
		matches = regexpPattern.FindStringSubmatch(string(fileContent))
		if len(matches) != 0 {
			c.NumberOfInvestors = strings.Count(matches[1], ",")
		}

		numberOfPriorInvestorsPattern := `Prior Investors\n([\s\S]+?)\n\n`
		regexpPattern = regexp.MustCompile(numberOfPriorInvestorsPattern)
		matches = regexpPattern.FindStringSubmatch(string(fileContent))
		if len(matches) != 0 {
			priorInvestorSubPattern := `(.+?),`
			priorInvestors := regexp.MustCompile(priorInvestorSubPattern).FindAllStringSubmatch(matches[1], -1)
			c.NumberOfPriorInvestors = len(priorInvestors)
		}

		numberOfCompetitorsPattern := `Competitors\n([\s\S]+?)\n\n`
		regexpPattern = regexp.MustCompile(numberOfCompetitorsPattern)
		matches = regexpPattern.FindStringSubmatch(string(fileContent))
		if len(matches) != 0 {
			competitorsBlock := strings.ReplaceAll(matches[1], ", LLC", "")
			competitorsBlock = strings.ReplaceAll(competitorsBlock, ", Inc.", "")
			c.NumberOfCompetitors = strings.Count(competitorsBlock, ",") + 1
		}

	default:
		c.CompanyType = "Private"

		employeesPattern := `Global Number of Employees \(Latest\): (\d+)`
		regexpPattern = regexp.MustCompile(employeesPattern)
		matches := regexpPattern.FindStringSubmatch(string(fileContent))
		if len(matches) != 0 {
			c.Employees = matches[1]
		}

		numberOfInvestorsPattern := `Current and Pending Investors\n([\s\S]+?)\n\nFinancial Information`
		regexpPattern = regexp.MustCompile(numberOfInvestorsPattern)
		matches = regexpPattern.FindStringSubmatch(string(fileContent))
		if len(matches) != 0 {
			investorSubPattern := `\b\w+-\d+-\d+\b`
			investors := regexp.MustCompile(investorSubPattern).FindAllString(matches[1], -1)
			c.NumberOfInvestors = len(investors)
		}

		numberOfPriorInvestorsPattern := `Prior Investors\n([\s\S]+?)\n\n`
		regexpPattern = regexp.MustCompile(numberOfPriorInvestorsPattern)
		matches = regexpPattern.FindStringSubmatch(string(fileContent))
		if len(matches) != 0 {
			c.NumberOfPriorInvestors = strings.Count(matches[1], "\n") + 1
		}

		// TODO: for private companies, the number of competitors is not available in the file
		// instead it is available on a separate page in the left margin of S&P capital IQ
		// download these and parse them separately like we do with alliances

	}

	allianceFileName := "./S&P/" + company + " SA.txt"
	allianceFileContent, err := ioutil.ReadFile(allianceFileName)
	if err != nil {
		return c, err
	}

	// TODO: the regex to get the number of alliances doesn't work. Fix it below.
	// numberOfAlliances is extracted from a different file
	numberOfAlliancesPattern := `Alliances\n([^\n]+)`
	regexpPattern = regexp.MustCompile(numberOfAlliancesPattern)
	matches = regexpPattern.FindStringSubmatch(string(allianceFileContent))
	if len(matches) != 0 {
		c.NumberOfAlliances = strings.Count(matches[1], "\n")
	}

	return c, nil
}

func writeCompanyToDatabase(db *sql.DB, c Company) error {
	_, err := db.Exec(`UPDATE companies SET employees = ?, foundingYear = ?, industry = ?, 
	numberOfInvestors = ?, revenue = ?, netIncome = ?, companyType = ?, 
	numberOfPriorInvestors = ?, numberOfCompetitors = ?, numberOfAlliances = ? WHERE name = ?;`,
		c.Employees, c.FoundingYear, c.Industry, c.NumberOfInvestors, c.Revenue,
		c.NetIncome, c.CompanyType, c.NumberOfPriorInvestors, c.NumberOfCompetitors, c.NumberOfAlliances,
		c.Name)
	if err != nil {
		return fmt.Errorf("couldn't update %v in companies table: %v", c.Name, err)
	}

	return nil
}
