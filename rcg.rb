File.open("randomized.csv", "w") { |f|
  lines, i, strings = 500000, 0, ""
  while i < lines do
    value = rand(36**8).to_s(36)
    startDate = "1504606943055"
    expiryDate = "1527675743055"
    campaignId = "1"
    
    strings << value
    strings << ","
    strings << startDate
    strings << ","
    strings << expiryDate
    strings << ","
    strings << campaignId
    strings << "\n"

    i += 1
  end 

  f.write(strings)
}