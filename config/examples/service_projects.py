import langextract as lx

EXAMPLES = [
    lx.data.ExampleData(
        text="The patient suffers from chronic diseases, osteoporosis, and deformity in her knees. She needs a knee joint replacement surgery and physical therapy. The treatment costs AED 15,500 for the examinations and surgery, AED 13,300 for accommodation and living ",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Knee Joint Replacement Surgery and Physical Therapy for Chronic Knee Conditions"),
            lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="1"),
            lx.data.Extraction(extraction_class="beneficiary_group_name", extraction_text="Patient"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Examinations and surgery"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Provision of medical treatment for a patient with chronic conditions, including knee joint replacement surgery and associated medical examinations, followed by required physical therapy."),
            lx.data.Extraction(extraction_class="item", extraction_text="Surgery"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Healthcare"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="1"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Unit"),
            lx.data.Extraction(extraction_class="project_amount_extracted", extraction_text="15500"),

    
            lx.data.Extraction(extraction_class="project_title", extraction_text="Accommodation and living"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Provision of accommodation and living support for a patient undergoing medical treatment to ensure appropriate care and recovery during the treatment period."),
            lx.data.Extraction(extraction_class="item", extraction_text="Accommodation"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Shelter"),
            lx.data.Extraction(extraction_class="project_amount_extracted", extraction_text="13300"),
        ]
    ),
    lx.data.ExampleData(
        text="We received a report from the Authority’s office in the West Bank to complete the Al-Aqsa Kindergarten project as follows: 1- Approval of the disbursement of the previous contractor’s entitlements at an amount of AED 293,760 2- Approval of the disbursemen",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Completion of the Al-aqsa Kindergarten Project"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Approval of the disbursement of the previous contractor entitlements"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Administrative and financial actions to support the completion of the Al-Aqsa Kindergarten project, including approval and disbursement of outstanding contractor entitlements based on a report received from the Authority’s office in the West Bank."),
            lx.data.Extraction(extraction_class="project_amount_extracted", extraction_text="293760"),
        ]
    ),
    lx.data.ExampleData(
        text="The patient suffers from lymphoma and is receiving chemotherapy sessions in Egypt at her own expense. The patient needs to complete chemotherapy and be re-evaluated after three sessions. The cost of the remaining three sessions is AED 2,000, while accommo",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Chemotherapy Completion and Accommodation Support for Lymphoma Patient in Egypt"),
            lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="1"),
            lx.data.Extraction(extraction_class="beneficiary_group_name", extraction_text="Patient"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Remaining Chemotherapy Sessions"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Provision of medical support for a patient with lymphoma through the completion of remaining chemotherapy sessions, including required medical evaluation following treatment."),
            lx.data.Extraction(extraction_class="item", extraction_text="Chemotherapy Sessions"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Healthcare"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="3"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Unit"),
            lx.data.Extraction(extraction_class="project_amount_extracted", extraction_text="2000"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Accommodation During Chemotherapy"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Provision of accommodation and living support for a patient undergoing chemotherapy treatment to ensure continuity of care during the treatment period."),
            lx.data.Extraction(extraction_class="item", extraction_text="Accommodation"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Shelter"),
        ]
    ),
    lx.data.ExampleData(
        text="Treatment of patients (Thailand, Egypt, Spain, Germany, USA)",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Treatment of patients in multiple countries"),
            lx.data.Extraction(extraction_class="beneficiary_group_name", extraction_text="Patient"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Treatment of patients in multiple countries"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Provision of medical treatment for patients through access to healthcare services in Thailand, Egypt, Spain, Germany, and the United States."),
            lx.data.Extraction(extraction_class="item", extraction_text="Treatment"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Healthcare"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="1"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Unit"),
        ]
    )
]