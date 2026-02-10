import langextract as lx

EXAMPLES = [
    lx.data.ExampleData(
        text=" 3 classroom school made of bricks with zinc roof with 3 bathrooms + furnishing (120 students) - 150 square meters",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Construction of School with Facilities"),
            lx.data.Extraction(extraction_class="project_title", extraction_text="Construction of School with Facilities"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Construction and furnishing of a three-classroom brick school with a zinc roof, including three bathrooms, covering a total area of 150 square meters to support educational needs for students."),
            lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="120"),
            lx.data.Extraction(extraction_class="beneficiary_group_name", extraction_text="Students"),
            lx.data.Extraction(extraction_class="asset", extraction_text="School"),
            lx.data.Extraction(extraction_class="asset_category", extraction_text="School"),
            lx.data.Extraction(extraction_class="asset_quantity", extraction_text="3"),            
        ]
    ),
    lx.data.ExampleData(
        text=" A mosque of 90 worshipers made of brick with a zinc roof, with a minaret + mats + 2 bathrooms + a place for ablution - 56 square meters",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Construction of Mosque with Facilities"),
            lx.data.Extraction(extraction_class="project_title", extraction_text="Construction of Mosque with Facilities"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Construction of a brick mosque with a zinc roof, including a minaret, prayer mats, two bathrooms, and an ablution area, with a total built area of 56 square meters to serve worshippers."),
            lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="90"),
            lx.data.Extraction(extraction_class="beneficiary_group_name", extraction_text="Worshippers"),
            lx.data.Extraction(extraction_class="asset", extraction_text="Mosque"),
            lx.data.Extraction(extraction_class="asset_category", extraction_text="Mosque"),
            lx.data.Extraction(extraction_class="asset_quantity", extraction_text="1"),          
        ]
    ),
    lx.data.ExampleData(
        text=" A school consisting of 1 classroom made of bricks with a zinc roof, accommodating 40 students, furnishing + 1 bathroom, 50 square meters.",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Construction of School with Facilities"),
            lx.data.Extraction(extraction_class="project_title", extraction_text="Construction of School with Facilities"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Construction and furnishing of a one-classroom brick school with a zinc roof, including one bathroom, covering a total area of 50 square meters to support students’ education."),
            lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="40"),
            lx.data.Extraction(extraction_class="beneficiary_group_name", extraction_text="Students"),
            lx.data.Extraction(extraction_class="asset", extraction_text="School"),
            lx.data.Extraction(extraction_class="asset_category", extraction_text="School"),
            lx.data.Extraction(extraction_class="asset_quantity", extraction_text="1"),            
        ]
    ),
    lx.data.ExampleData(
        text=" Artesian well with electric pump 1000 liter water tank | Beneficiaries: 500 people Depth from 200 meters | Shelf life is 15 years",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Construction of Artesian Well with Electric Pump"),
            lx.data.Extraction(extraction_class="project_title", extraction_text="Construction of Artesian Well with Electric Pump"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Construction of an artesian well equipped with an electric pump and a 1,000-liter water storage tank, drilled to a depth of 200 meters, to provide sustainable water access for the community with an expected operational lifespan of 15 years."),
            lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="500"),
            lx.data.Extraction(extraction_class="beneficiary_group_name", extraction_text="People"),
            lx.data.Extraction(extraction_class="asset", extraction_text="Artesian well with electric pump"),
            lx.data.Extraction(extraction_class="asset_category", extraction_text="Well"),
            lx.data.Extraction(extraction_class="asset_quantity", extraction_text="1"),   
            lx.data.Extraction(extraction_class="asset_capacity", extraction_text="1000"), 
            lx.data.Extraction(extraction_class="asset_capacity_uom", extraction_text="Liter"),            
        ]
    ),
    lx.data.ExampleData(
        text=" Borehole with electric pump 1000 liter water tank | 3 faucets | Depth 30-40 meters | Life span 5-10 years | Beneficiaries: 10 families",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Construction of Borehole with Water Tank and Faucets"),
            lx.data.Extraction(extraction_class="project_title", extraction_text="Construction of Borehole with Water Tank and Faucets"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Construction of a borehole equipped with an electric pump, a 1,000-liter water storage tank, and three faucets, drilled to a depth of 30–40 meters, to provide reliable water access with an estimated operational lifespan of 5–10 years."),
            lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="10"),
            lx.data.Extraction(extraction_class="beneficiary_group_name", extraction_text="Families"),
            lx.data.Extraction(extraction_class="asset", extraction_text="Borehole with electric pump"),
            lx.data.Extraction(extraction_class="asset_category", extraction_text="Well"),
            lx.data.Extraction(extraction_class="asset_quantity", extraction_text="1"),   
            lx.data.Extraction(extraction_class="asset_capacity", extraction_text="1000"), 
            lx.data.Extraction(extraction_class="asset_capacity_uom", extraction_text="Liter"),         
        ]
    ),
    lx.data.ExampleData(
        text="Construction of (2) Schools + maintenance of (9) schools with four attached offices ",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Construction and Maintenance of Schools"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Construction of Schools"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Construction of two school buildings to expand access to educational infrastructure."),
            lx.data.Extraction(extraction_class="asset", extraction_text="School"),
            lx.data.Extraction(extraction_class="asset_category", extraction_text="School"),
            lx.data.Extraction(extraction_class="asset_quantity", extraction_text="2"),            

            lx.data.Extraction(extraction_class="project_title", extraction_text="Maintenance of Schools"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Maintenance and rehabilitation of nine existing school buildings, including four attached office facilities, to improve and sustain educational operations."),
            lx.data.Extraction(extraction_class="asset", extraction_text="School"),
            lx.data.Extraction(extraction_class="asset_category", extraction_text="School"),
            lx.data.Extraction(extraction_class="asset_quantity", extraction_text="9"),            
        ]
    ),
    lx.data.ExampleData(
        text="A plastic water tank with a asset_capacity of 522 gallons (2 records)",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Construction of a plastic water tank"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Construction of a plastic water tank"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Provision of a plastic water storage tank with a capacity of 522 gallons to support water storage needs."),
            lx.data.Extraction(extraction_class="asset", extraction_text="Water Tank"),
            lx.data.Extraction(extraction_class="asset_category", extraction_text="Water Tank"),
            lx.data.Extraction(extraction_class="asset_quantity", extraction_text="1"), 
            lx.data.Extraction(extraction_class="asset_capacity", extraction_text="522"), 
            lx.data.Extraction(extraction_class="asset_capacity_uom", extraction_text="Gallon"),              
        ]
    )
    ]