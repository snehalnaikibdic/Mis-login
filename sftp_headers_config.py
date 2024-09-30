class SFTP_API_HEADERS:
    entity_registration = ['channel', 'hubId', 'idpId', 'requestID', 'noOfEntities', 'entityCode', 'noOfIdentifiers',
                        'entityIdType', 'entityIdNo', 'entityIdName', 'ifsc']
    invoice_reg_with_entity_code = ['channel', 'hubId', 'idpId', 'requestID', 'groupingId', 'sellerCode', 'buyerCode',
                        'sellerGst', 'buyerGst', 'noOfInvoices', 'validationType', 'validationRefNo', 'invoiceNo',
                        'invoiceDate', 'invoiceAmt', 'verifyGSTNFlag', 'invoiceDueDate']
    invoice_reg_without_entity_code = ['channel', 'hubId', 'idpId', 'requestID', 'groupingId', 'sellerGst', 'buyerGst', 'noOfInvoices',
                        'validationType', 'validationRefNo', 'invoiceNo', 'invoiceDate', 'invoiceAmt',
                        'verifyGSTNFlag', 'invoiceDueDate', 'sellerDataNo', 'sellerIdType', 'sellerIdNo',
                        'sellerIdName', 'sellerIfsc', 'buyerDataNo', 'buyerIdType',
                        'buyerIdNo', 'buyerIdName', 'buyerIfsc']
    finance = ["channel", "hubId", "idpId", "requestID", "ledgerNo", "ledgerAmtFlag", "lenderCategory",
                            "lenderName", "lenderCode",	"borrowerCategory",	"noOfInvoices",	"validationType",
                            "validationRefNo", "invoiceNo", "invoiceDate", "invoiceAmt", "financeRequestAmt",
                            "financeRequestDate", "dueDate", "fundingAmtFlag", "adjustmentType", "adjustmentAmt"]
    cancel = ['channel', 'hubId', 'idpId', 'requestID', 'ledgerNo', 'cancellationReason']
    disbursement = ['channel', 'hubId', 'idpId', 'requestID', 'ledgerNo', 'lenderCategory', 'lenderName',
                        'lenderCode', 'noOfInvoices', "validationType", "validationRefNo", 'invoiceNo', 'invoiceDate',
                        'invoiceAmt', 'disbursedFlag', 'disbursedAmt', 'disbursedDate', 'dueAmt', 'dueDate']
    repayment = ['channel', 'hubId', 'idpId', 'requestID', 'ledgerNo', 'borrowerCategory', 'noOfInvoices',
                        "validationType", "validationRefNo", 'invoiceNo', 'invoiceDate', 'invoiceAmt', 'dueAmt',
                        'dueDate', 'assetClassification', 'repaymentType', 'repaymentFlag', 'repaymentAmt',
                        'repaymentDate', 'pendingDueAmt', 'dpd']
    validate_with_entity_code = ['channel', 'hubId', 'idpId', 'requestID',  'groupingId', 'sellerCode', 'buyerCode',
                                 'sellerGst', 'buyerGst', 'noOfInvoices', 'invoiceNo', 'validationType',
                                 'validationRefNo', 'invoiceDate', 'invoiceAmt', 'verifyGSTNFlag', 'invoiceDueDate']
    validate_without_entity_code = ['channel', 'hubId', 'idpId', 'requestID', 'groupingId', 'sellerGst', 'buyerGst',
                                    'noOfInvoices', 'invoiceNo', 'validationType', 'validationRefNo', 'invoiceDate',
                                    'invoiceAmt', 'verifyGSTNFlag', 'invoiceDueDate', 'sellerDataNo', 'sellerIdType',
                                    'sellerIdNo', 'sellerIdName', 'sellerIfsc', 'buyerDataNo', 'buyerIdType',
                                    'buyerIdNo', 'buyerIdName', 'buyerIfsc']
    invoice_reg_with_entity_code_finance = ['channel', 'hubId', 'idpId', 'requestID', 'groupingId', 'sellerCode', 'buyerCode',
                        'sellerGst', 'buyerGst', 'ledgerAmtFlag', 'lenderCategory', 'lenderName', 'lenderCode',
                        'borrowerCategory', 'noOfInvoices', 'validationType', 'validationRefNo', 'invoiceNo',
                        'invoiceDate', 'invoiceAmt', 'verifyGSTNFlag', 'invoiceDueDate', 'financeRequestAmt',
                        'financeRequestDate', 'dueDate', 'fundingAmtFlag', 'adjustmentType', 'adjustmentAmt']
    invoice_reg_with_entity_code_finance_disbursement = ["channel", "hubId", "idpId", "requestID", "groupingId", "sellerCode", "buyerCode",
                            "sellerGst", "buyerGst", "ledgerAmtFlag", "lenderCategory", "lenderName", "lenderCode",
                            "borrowerCategory", "noOfInvoices", "validationType", "validationRefNo", "invoiceNo",
                            "invoiceDate", "invoiceAmt", "verifyGSTNFlag", "invoiceDueDate", "financeRequestAmt",
                            "financeRequestDate", "dueDate", "fundingAmtFlag", "adjustmentType", "adjustmentAmt",
                            "disbursedFlag", "disbursedAmt", "disbursedDate", "dueAmt"]
    invoice_reg_without_entity_code_finance = ['channel', 'hubId', 'idpId', 'requestID', 'groupingId', 'sellerGst', 'buyerGst',
                        'ledgerAmtFlag', 'lenderCategory', 'lenderName', 'lenderCode', 'borrowerCategory',
                        'noOfInvoices', 'validationType', 'validationRefNo', 'invoiceNo', 'invoiceDate',
                        'invoiceAmt', 'verifyGSTNFlag', 'invoiceDueDate', 'sellerDataNo', 'sellerIdType',
                        'sellerIdNo', 'sellerIdName', 'sellerIfsc', 'buyerDataNo', 'buyerIdType', 'buyerIdNo',
                        'buyerIdName', 'buyerIfsc', 'financeRequestAmt', 'financeRequestDate', 'dueDate',
                        'fundingAmtFlag', 'adjustmentType', 'adjustmentAmt']
    invoice_reg_without_entity_code_finance_disbursement = ["channel", "hubId", "idpId", "requestID",
                                                                     "groupingId", "sellerGst", "buyerGst",
                                                                     "ledgerAmtFlag", "lenderCategory", "lenderName",
                                                                     "lenderCode", "borrowerCategory",
                                                                     "noOfInvoices", "validationType",
                                                                     "validationRefNo", "invoiceNo", "invoiceDate",
                                                                     "invoiceAmt", "verifyGSTNFlag", "invoiceDueDate",
                                                                     "sellerDataNo", "sellerIdType", "sellerIdNo",
                                                                     "sellerIdName", "sellerIfsc", "buyerDataNo",
                                                                     "buyerIdNo", "buyerIdType", "buyerIdName",
                                                                     "buyerIfsc", "financeRequestAmt",
                                                                     "financeRequestDate", "dueDate", "fundingAmtFlag",
                                                                     "adjustmentType", "adjustmentAmt",
                                                                     "disbursedFlag", "disbursedAmt", "disbursedDate",
                                                                     "dueAmt"]
    disbursement_repayment = ["channel", "hubId",	"idpId", "requestID", "ledgerNo", "lenderCategory",	"lenderName",
                            "lenderCode", "borrowerCategory", "noOfInvoices", "validationType", "validationRefNo",
                            "invoiceNo", "invoiceDate", "invoiceAmt", "disbursedFlag", "disbursedAmt", "disbursedDate",
                            "dueAmt",	"dueDate", "assetClassification", "repaymentType", "repaymentFlag",
                            "repaymentAmt", "repaymentDate", "pendingDueAmt", "dpd"]

    api_with_cols = {
        'invoice_reg_without_entity_code_finance_disbursement.csv': invoice_reg_without_entity_code_finance_disbursement,
        'disbursement.csv': disbursement,
        'repayment.csv': repayment,
        'invoice_reg_without_entity_code_finance.csv': invoice_reg_without_entity_code_finance,
        'finance.csv': finance,
        'disbursement_repayment.csv': disbursement_repayment,
        'invoice_reg_without_entity_code.csv': invoice_reg_without_entity_code,
        'invoice_reg_with_entity_code.csv':  invoice_reg_with_entity_code ,
        'invoice_reg_with_entity_code_finance.csv': invoice_reg_with_entity_code_finance,
        'invoice_reg_with_entity_code_finance_disbursement.csv': invoice_reg_with_entity_code_finance_disbursement,
        'cancel.csv': cancel,
        'entity_registration.csv': entity_registration,
        'validate_with_entity_code.csv':  validate_with_entity_code,
        'validate_without_entity_code.csv':  validate_without_entity_code
    }