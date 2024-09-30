# If no date is given the output will be,
{
  "requestId": "12345",
  "disbursedAmount": 10000,
  "disbursedDate": "2024-01-10",
  "repaymentDate": None,
  "status": "Disbursed",
  "repaymentStatus": "Pending",
  "signature": "some_generated_signature"
}
# If the disbursement date is 10th Jan, 2024, the repayment date is 20th Feb, 2024 and if 
# checked on 20th Jan, 2024, the output will be,
{
  "requestId": "12345",
  "disbursedAmount": 10000,
  "disbursedDate": "2024-01-10",
  "repaymentDate": "2024-02-20",
  "status": "Disbursed",
  "repaymentStatus": "Pending",
  "signature": "some_generated_signature"
}
