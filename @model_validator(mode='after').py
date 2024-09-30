@model_validator(mode='after')
def validate_field(self):
        if str(self.validationType) == '' and str(self.validationRefNo) != '':
            raise HTTPException(status_code=400, detail="validationType should not be blank")
        if str(self.validationRefNo) == '' and str(self.validationType) != '':
            raise HTTPException(status_code=400, detail="validationRefNo should not be blank")
        if str(self.validationRefNo) != '':
            validate_string = re.search(validation_ref_no_pattern, str(self.validationRefNo))
            if not validate_string:
                raise HTTPException(status_code=400, detail=f"validationRefNo can not accept special character {self.validationRefNo}")
        if not special_char_pattern.match(str(self.assetClassification)):
            raise HTTPException(status_code=400, detail=f"assetClassification can not accept special character {self.assetClassification}")
        if not special_char_pattern.match(str(self.disbursedFlag)):
            raise HTTPException(status_code=400, detail=f"disbursedFlag can not accept special character {self.disbursedFlag}")
        if not date_special_char_pattern.match(self.disbursedDate):
            raise HTTPException(status_code=400, detail=f"disbursedDate can not accept special character {self.disbursedDate}")
        if not date_special_char_pattern.match(str(self.dueDate)):
            raise HTTPException(status_code=400, detail=f"dueDate can not accept special character {self.dueDate}")
        if not special_char_pattern.match(self.repaymentType):
            raise HTTPException(status_code=400, detail=f"repaymentType can not accept special character {self.repaymentType}")
        if not special_char_pattern.match(str(self.repaymentFlag)):
            raise HTTPException(status_code=400, detail=f"repaymentFlag can not accept special character {self.repaymentFlag}")
        if not date_special_char_pattern.match(str(self.repaymentDate)):
            raise HTTPException(status_code=400, detail=f"repaymentDate can not accept special character {self.repaymentDate}")
        if not special_char_pattern.match(str(self.dpd)):
            raise HTTPException(status_code=400, detail=f"dpd can not accept special character {self.dpd}")
        if not date_special_char_pattern.match(str(self.invoiceDate)):
            raise HTTPException(status_code=400, detail=f"invoiceDate can not accept special character {self.invoiceDate}")
        check_decimal_precision('disbursedAmt', str(self.disbursedAmt))
        check_decimal_precision('dueAmt', str(self.dueAmt))
        check_decimal_precision('pendingDueAmt', str(self.pendingDueAmt))
        check_decimal_precision('invoiceAmt', str(self.invoiceAmt))
        check_decimal_precision('repaymentAmt', str(self.repaymentAmt))
        return self

@field_validator('validationType')
def validate_validation_type(cls, value: str):
        # if value == '':
        #     raise HTTPException(status_code=400, detail="validationType can not be blank")
        if value != "":
            if not value.lower() in ('einvoice', 'ewaybill', 'gstfiling'):
                raise HTTPException(status_code=400, detail=f"validationType can be eInvoice, eWayBill or gstFiling {value}")
        return value

    # @field_validator('validationRefNo')
    # def validate_validation_ref_no(cls, value: str):
    #     # if value == '':
    #     #     raise HTTPException(status_code=400, detail="validationRefNo can not be blank")
    #     if value != "":
    #         if len(value) > 100:
    #             raise HTTPException(status_code=400, detail="validationRefNo can not be greater than 100")
    #     return value

@field_validator('invoiceNo')
def validate_invoice_no(cls, value: str):
        if value == '':
            raise HTTPException(status_code=400, detail="InvoiceNo can not be blank")
        elif len(value) > 100:
            raise HTTPException(status_code=400, detail=f"InvoiceNo can not be greater than 100 {value}")
        elif not re.match(invoice_number_pattern, value):
            raise HTTPException(status_code=400, detail=f"Invalid invoice number format. Use only letters, numbers, spaces, and symbols: '-', '_', '/', '#'. Please check your input.")
        return value

@field_validator('assetClassification')
def validate_asset_classification(cls, value: str):
        if len(value) > 250:
            raise HTTPException(status_code=400, detail=f"assetClassification can not be greater than 250 {value}")
        return value

@field_validator('dueAmt')
def validate_due_amt(cls, value: str):
        if value == '':
            raise HTTPException(status_code=400, detail="dueAmt can not be blank")
        elif len(str(value)) > 20:
            raise HTTPException(status_code=400, detail=f"dueAmt can not be greater than 20 digit {value}")
        return value

@field_validator('disbursedFlag')
def validate_disbursed_flag(cls, value: str):
        if value == '':
            raise HTTPException(status_code=400, detail="disbursedFlag can not be blank")
        else:
            if not value.lower() in ('full', 'partial'):
                raise HTTPException(status_code=400, detail=f"disbursedFlag can be full or partial {value}")
        return value

@field_validator('disbursedAmt')
def validate_disbursed_amt(cls, value: str):
        if value == '':
            raise HTTPException(status_code=400, detail="disbursedAmt can not be blank")
        elif len(str(value)) > 20:
            raise HTTPException(status_code=400, detail=f"disbursedAmt can not be greater than 20 digit {value}")
        return value

@field_validator('disbursedDate')
def validate_disbursed_date(cls, value: date) -> date:
        if value == '':
            raise HTTPException(status_code=400, detail="disbursed date can not be blank")
        elif len(str(value)) > 10:
            raise HTTPException(status_code=400, detail=f"disbursedDate can not be greater than 10 {value}")
        elif value != '':
            from datetime import datetime
            try:
                dd = datetime.strptime(value, "%d/%m/%Y")
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"date format accept d/m/Y format {value}")
        return value

@field_validator('dueDate')
def validate_due_date(cls, value: date) -> date:
        if value == '':
            raise HTTPException(status_code=400, detail="dueDate can not be blank")
        elif len(str(value)) > 10:
            raise HTTPException(status_code=400, detail=f"dueDate can not be greater than 10 {value}")
        elif value != '':
            try:
                dd = datetime.strptime(value, "%d/%m/%Y")
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"date format except d/m/Y format {value}")
        return value

@field_validator('repaymentType')
def validate_repayment_type(cls, value: str):
        if len(value) > 20:
            raise HTTPException(status_code=400, detail=f"repaymentType can not be greater than 20 {value}")
        return value

@field_validator('repaymentAmt')
def validate_repayment_amt(cls, value: str):
        if value == '':
            raise HTTPException(status_code=400, detail="repaymentAmt can not be blank")
        elif len(str(value)) > 20:
            raise HTTPException(status_code=400, detail=f"repaymentAmt can not be greater than 20 digit {value}")
        return value

@field_validator('repaymentDate')
def validate_repayment_date(cls, value: date) -> date:
        if value == '':
            raise HTTPException(status_code=400, detail="repaymentDate can not be blank")
        elif len(str(value)) > 10:
            raise HTTPException(status_code=400, detail=f"repaymentDate can not be greater than 10 {value}")
        elif value != '':
            try:
                dd = datetime.strptime(value, "%d/%m/%Y")
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"date format accept d/m/Y format {value}")
        return value

@field_validator('repaymentFlag')
def validate_repayment_flag(cls, value: str):
        if value == '':
            raise HTTPException(status_code=400, detail="repaymentFlag can not be blank")
        elif len(value) > 20:
            raise HTTPException(status_code=400, detail=f"repaymentFlag can not be greater than 20 {value}")
        else:
            if not value.lower() in ('full', 'partial'):
                raise HTTPException(status_code=400, detail=f"repayment flag can be full or partial {value}")
        return value

@field_validator('pendingDueAmt')
def validate_pending_due_amount(cls, value: int):
        if value == '':
            raise HTTPException(status_code=400, detail="pendingDueAmt can not be blank")
        elif len(str(value)) > 20:
            raise HTTPException(status_code=400, detail=f"pendingDueAmt can not be greater than 20 digit {value}")
        return value

@field_validator('dpd')
def validate_dpd(cls, value: str):
        if value == '':
            raise HTTPException(status_code=400, detail="dpd can not be blank")
        elif len(str(value)) > 10:
            raise HTTPException(status_code=400, detail=f"dpd can not be greater than 10{value}")
        return value

@field_validator('invoiceDate')
def validate_invoice_date(cls, value: date) -> date:
        if value == '':
            raise HTTPException(status_code=400, detail="InvoiceDate can not be blank")
        elif len(str(value)) > 10:
            raise HTTPException(status_code=400, detail=f"InvoiceDate can not be greater than 10 {value}")
        elif value != '':
            try:
                dd = datetime.strptime(value, "%d/%m/%Y")
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"date format except d/m/Y format {value}")
        return value

@field_validator('invoiceAmt')
def validate_invoice_amount(cls, value: int):
        if value == '':
            raise HTTPException(status_code=400, detail="invoiceAmt can not be blank")
        elif len(str(value)) > 20:
            raise HTTPException(status_code=400, detail=f"invoiceAmt can not be greater than 20 digit {value}")
        return value
