package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// Company is the model for the data we are extracting from the files
// most of the data is pulled from the company tearsheet file from S&P Capital IQ
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
	NumberOfCompetitors    int // for private companies, this is not in the tearsheet file. It is in the competitor file
	NumberOfAlliances      int // this is not in the tearsheet file. It is in the strategic alliances file
}

func main() {

	// initialize the database
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
		// convert the company name to lowercase because the file names are all lowercase
		// and we want to avoid mismatches between the file names and the company names in the database
		company = strings.ToLower(company)
		var c Company
		c, err := extractDataFromCompanyFile(company)
		if err != nil {
			log.Println(err)
			continue
		}
		c.Name = company

		if err := writeCompanyToDatabase(db, c); err != nil {
			log.Println(err)
			continue
		}
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

	// extract data that has common regex patterns across tearsheet files
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

		// for private companies, numberOfCompetitors is extracted from a different file
		// we don't expect all companies to have this page so we don't throw an error. Just log it.
		competitorFileName := "./S&P/" + company + " C.txt"
		competitorFileContent, err := ioutil.ReadFile(competitorFileName)
		if err != nil {
			log.Println(err)
			return c, nil
		} else {
			numberOfCompetitorsPattern := `Business Description:`
			regexpPattern := regexp.MustCompile(numberOfCompetitorsPattern)
			matches := regexpPattern.FindAllStringIndex(string(competitorFileContent), -1)
			if matches != nil {
				c.NumberOfCompetitors = len(matches)
			}
		}

	}

	// numberOfAlliances is also extracted from a different file
	allianceFileName := "./S&P/" + company + " sa.txt"
	allianceFileContent, err := ioutil.ReadFile(allianceFileName)
	if err != nil {
		log.Println(err)
		return c, nil
	} else {
		numberOfAlliancesPattern := `Business Description:`
		regexpPattern := regexp.MustCompile(numberOfAlliancesPattern)
		matches := regexpPattern.FindAllStringIndex(string(allianceFileContent), -1)
		if matches != nil {
			c.NumberOfAlliances = len(matches)
		}
	}

	return c, nil
}

func writeCompanyToDatabase(db *sql.DB, c Company) error {
	_, err := db.Exec(`UPDATE companies SET employees = ?, foundingYear = ?, industry = ?, 
	numberOfInvestors = ?, revenue = ?, netIncome = ?, companyType = ?, 
	numberOfPriorInvestors = ?, numberOfCompetitors = ?, numberOfAlliances = ? WHERE lower(name) = ?;`,
		c.Employees, c.FoundingYear, c.Industry, c.NumberOfInvestors, c.Revenue,
		c.NetIncome, c.CompanyType, c.NumberOfPriorInvestors, c.NumberOfCompetitors, c.NumberOfAlliances,
		c.Name)
	if err != nil {
		return fmt.Errorf("couldn't update %v in companies table: %v", c.Name, err)
	}

	return nil
}
