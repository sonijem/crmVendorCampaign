Feature: Process campaign and engagement data

  Scenario: Generate reports from campaign and engagement data
    Given the campaign and engagement data is available
    When the data is processed
    Then the current campaign engagement report is generated
    And the campaign overview report is generated
