import langextract as lx

EXAMPLES = [
    lx.data.ExampleData(
        text="15 tons of tents, 5 tons of blankets and food supplies for the people of Socotra affected by Cyclone Mkono, 10,000 food baskets containing essential foodstuffs (9 items) for the people of Socotra affected by Cyclone Mkono",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Aid for the people of Socotra affected by Cyclone Mkono"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Aid for the people of Socotra affected by Cyclone Mkono"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Provision and distribution of emergency relief supplies, including 15 tons of tents, 5 tons of blankets, and 10,000 food baskets containing essential food items, to support communities in Socotra affected by Cyclone Mkono"),

            lx.data.Extraction(extraction_class="beneficiary_group_name", extraction_text="People"),

            lx.data.Extraction(extraction_class="item", extraction_text="Tent"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Relief Items"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="15"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="ton"),
            
            lx.data.Extraction(extraction_class="item", extraction_text="Blanket"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Relief Items"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="5"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="ton"),
            
            lx.data.Extraction(extraction_class="item", extraction_text="Food Baskets"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="10000"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="unit"),
        ]
    ),
    lx.data.ExampleData(
        text=" Distribution of (1060) food basket from Najjar complex warehouse and logistic operations of the team in the governorates Hadramout / Marib / Mahra / Shabwa",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Distribution of Food Baskets from Najjar Complex Warehouse"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Distribution of Food Baskets from Najjar Complex Warehouse"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Distribution of 1,060 food baskets from the Najjar Complex warehouse, including logistics and field operations across the governorates of Hadramout, Marib, Mahra, and Shabwa."),
            lx.data.Extraction(extraction_class="item", extraction_text="Food Basket"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="1060"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Unit"),
        ]
    ),
    lx.data.ExampleData(
        text=" Providing a poor family with food (50 Kilogram of rice + 1 carton of pasta + 2 Kilogram of milk + 5 Liter of oil + 5 Kilogram of sugar + 1 kilogram of salt)",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Providing a Poor Family with Food"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Providing a Poor Family with Food"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Provision of a food assistance package to a vulnerable family, consisting of rice, pasta, milk, cooking oil, sugar, and salt to support basic nutritional needs."),

            lx.data.Extraction(extraction_class="beneficiary_count", extraction_text="1"),
            lx.data.Extraction(extraction_class="beneficiary_group_name", extraction_text="Family"),


            lx.data.Extraction(extraction_class="item", extraction_text="Rice"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="50"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Kilogram"),

            lx.data.Extraction(extraction_class="item", extraction_text="Pasta"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="1"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Carton"),

            lx.data.Extraction(extraction_class="item", extraction_text="Milk"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="2"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Kilogram"),

            lx.data.Extraction(extraction_class="item", extraction_text="Oil"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="5"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Liter"),

            lx.data.Extraction(extraction_class="item", extraction_text="Sugar"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="5"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Kilogram"),

            lx.data.Extraction(extraction_class="item", extraction_text="Salt"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="1"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Kilogram"),
        ]
    ),
    lx.data.ExampleData(
        text=" Providing a winter package for families consisting of a food basket (10 Kilogram of flour, 3 Liter of oil, 3 Kilogram of salt, 3 Kilogram of pasta, 5 Kilogram of potatoes, 3 Kilogram of sugar, 1 kilogram of tea) + a blanket + an oil heater + provid",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Providing a Winter Package for Families"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Providing a Winter Package for Families"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Provision of a winter assistance package to families, including a food basket, blankets, and oil heaters, to support household needs during the winter season."),
            lx.data.Extraction(extraction_class="beneficiary_group_name", extraction_text="Families"),


            lx.data.Extraction(extraction_class="item", extraction_text="Flour"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="10"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Kilogram"),

            lx.data.Extraction(extraction_class="item", extraction_text="Oil"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="3"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Liter"),

            lx.data.Extraction(extraction_class="item", extraction_text="Salt"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="3"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Kilogram"),

            lx.data.Extraction(extraction_class="item", extraction_text="Pasta"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="3"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Kilogram"),

            lx.data.Extraction(extraction_class="item", extraction_text="Potatoes"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="5"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Kilogram"),

            lx.data.Extraction(extraction_class="item", extraction_text="Sugar"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="3"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Kilogram"),

            lx.data.Extraction(extraction_class="item", extraction_text="Tea"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="1"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Kilogram"),

            lx.data.Extraction(extraction_class="item", extraction_text="Blanket"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Relief Items"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="1"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Unit"),

            lx.data.Extraction(extraction_class="item", extraction_text="Oil Heater"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Relief Items"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="1"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Unit"),
        ]
    ),
    lx.data.ExampleData(
        text=" Shipments of 35 MT of Ready-to-use Therapeutic Food (RUTF) to Support United Nations Childrens Operations in Nigeria through the International Humanitarian City (IHC)",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Support United Nations Children's Operations in Nigeria"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Support United Nations Children's Operations in Nigeria"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Shipment of 35 metric tons of ready-to-use therapeutic food (RUTF) through the International Humanitarian City to support United Nations children’s humanitarian operations in Nigeria."),
            lx.data.Extraction(extraction_class="beneficiary_group_name", extraction_text="Children"),

            lx.data.Extraction(extraction_class="item", extraction_text="Ready-to-use Therapeutic Food"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="35"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Metric Ton"),
        ]
    ),
    lx.data.ExampleData(
        text="Consist of 2 flights, sent 26 tonnes of food aid to support Sudanese refugees in Chad affected by the conflict. In addition to logistic and support operations.",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Food Aid Support for Sudanese Refugees in Chad"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Food Aid Support for Sudanese Refugees in Chad"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Humanitarian airlift of food aid through two flights, delivering 26 metric tons of assistance to support Sudanese refugees in Chad affected by the conflict, including associated logistics and support operations."),
            lx.data.Extraction(extraction_class="beneficiary_group_name", extraction_text="Sudanese Refugees"),

            lx.data.Extraction(extraction_class="item", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="26"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Ton"),
        ]
    ),
    lx.data.ExampleData(
        text="Dispatching C 17 aircraft to transport (100) tons of foodstuff, presented by Khalifa Foundation (relied from 2015)",
        extractions=[
            lx.data.Extraction(extraction_class="master_project_title", extraction_text="Transportation of Foodstuff by C 17 Aircraft"),

            lx.data.Extraction(extraction_class="project_title", extraction_text="Transportation of Foodstuff by C 17 Aircraft"),
            lx.data.Extraction(extraction_class="project_description", extraction_text="Dispatch of a C-17 aircraft to transport 100 metric tons of food supplies provided by the Khalifa Foundation as humanitarian assistance."),

            lx.data.Extraction(extraction_class="item", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_category", extraction_text="Food"),
            lx.data.Extraction(extraction_class="item_quantity", extraction_text="100"),
            lx.data.Extraction(extraction_class="item_quantity_uom", extraction_text="Ton"),
        ]
    )
]