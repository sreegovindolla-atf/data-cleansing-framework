import langextract as lx

ATTR_EXAMPLES = [
    lx.data.ExampleData(
        text="YEAR: 2015 EMERGENCY_TITLE: لايوجد TITLE_EN: The Iraqi Students: Colleges cost, including accommodation, Education fees and health insurance DESC_EN: The Iraqi Students: Colleges cost, including accommodation, Education fees and health insurance TITLE_AR: الطلبة العراقيين: دفع فواتير الجامعات المتضمنة الرسوم الدراسية و السكن و التأمين الصحي للطلاب DESC_AR: الطلبة العراقيين: دفع فواتير الجامعات المتضمنة الرسوم الدراسية و السكن و التأمين الصحي للطلاب",
        extractions=[
            lx.data.Extraction(extraction_class="subsector_en", extraction_text="Secondary education"),        
            lx.data.Extraction(extraction_class="target_en", extraction_text="Achieve full and productive employment and decent work for all including women and young people")           
        ]
    ),
    lx.data.ExampleData(
        text="YEAR: 2002 EMERGENCY_TITLE: لايوجد TITLE_EN: Naser Complex DESC_EN: Naser Complex TITLE_AR: منشأة ناصر DESC_AR: منشأة ناصر",
        extractions=[
            lx.data.Extraction(extraction_class="subsector_en", extraction_text="Urban development and management")    ,      
            lx.data.Extraction(extraction_class="target_en", extraction_text="By 2020 to have achieved a significant improvement in the lives of at least 100 million slum dwellers")   
        ]
    ),
    lx.data.ExampleData(
        text="YEAR: 2014 EMERGENCY_TITLE: لايوجد TITLE_EN: Refrigerator to a poor family DESC_EN: Refrigerator to a poor family TITLE_AR: ثلاجة لاسرة فقيرة DESC_AR: ثلاجة لاسرة فقيرة",
        extractions=[
            lx.data.Extraction(extraction_class="subsector_en", extraction_text="Assorted basic social services")  , 
            lx.data.Extraction(extraction_class="target_en", extraction_text="Achieve full and productive employment and decent work for all including women and young people")          
        ]
    ),
    lx.data.ExampleData(
        text="YEAR: 2015 EMERGENCY_TITLE: Syrian Crisis TITLE_EN: Multi-Sector DESC_EN: Send the 16 convoy - the third installment of the 13 trucks loaded 325 tons of relief materials to Jordan TITLE_AR: مواد إغاثية متنوعة DESC_AR: إرسال القافلة البرية السادسة عشر - الدفعة الثالثة من (13) شاحنة نقلت 325 طن مواد إغاثية",
        extractions=[
            lx.data.Extraction(extraction_class="subsector_en", extraction_text="Emergency multi-sector aid") ,
            lx.data.Extraction(extraction_class="target_en", extraction_text="Halve between 1990 and 2015 the proportion of people whose income is less than one dollar a day")            
        ]
    ),
    lx.data.ExampleData(
        text="YEAR: 2015 EMERGENCY_TITLE: لايوجد TITLE_EN: Social Welfare Services Sponsring of poor families DESC_EN: 353 orphans for the period from 7/12 / 2015 TITLE_AR: خدمات الرعاية الإجتماعية (الكفالة ) DESC_AR: لعدد 2974 يتيم عن الفترة من 07 : 12 / 2015 م",
        extractions=[
            lx.data.Extraction(extraction_class="subsector_en", extraction_text="Social welfare services"),  
            lx.data.Extraction(extraction_class="target_en", extraction_text="Halve between 1990 and 2015 the proportion of people whose income is less than one dollar a day")           
        ]
    ),
    lx.data.ExampleData(
        text="YEAR: 2016 EMERGENCY_TITLE: لايوجد TITLE_EN: Equipment Hospital Ahmed bin Zayed Al Nahyan DESC_EN: Equipment Hospital Ahmed bin Zayed Al Nahyan TITLE_AR: أجهزة ومعدات مستشفى أحمد بن زايد آل نهيان DESC_AR: أجهزة ومعدات مستشفى أحمد بن زايد آل نهيان",
        extractions=[
            lx.data.Extraction(extraction_class="subsector_en", extraction_text="Medical services"),  
            lx.data.Extraction(extraction_class="target_en", extraction_text="Achieve universal health coverage, including financial risk protection, access to quality essential health-care services and access to safe, effective, quality and affordable essential medicines and vaccines for all")           
        ]
    ),
    lx.data.ExampleData(
        text="YEAR: 2016 EMERGENCY_TITLE: لايوجد TITLE_EN: Equipment birth and maternity hospital DESC_EN: Equipment birth and maternity hospital TITLE_AR: ‘ أجهزة ومعدات مستشفى الولادة والأمومة DESC_AR: ‘ أجهزة ومعدات مستشفى الولادة والأمومة",
        extractions=[
            lx.data.Extraction(extraction_class="subsector_en", extraction_text="Medical services"),  
            lx.data.Extraction(extraction_class="target_en", extraction_text="By 2030, reduce the global maternal mortality ratio to less than 70 per 100,000 live births")           
        ]
    ),
    lx.data.ExampleData(
        text="YEAR: 2017 EMERGENCY_TITLE: Yemen Crisis, 2015 TITLE_EN: Aid to the poor and the needy DESC_EN: Aid to the poor and the needy TITLE_AR: مساعدات للفقراء والمحتاجين DESC_AR: مساعدات للفقراء والمحتاجين",
        extractions=[
            lx.data.Extraction(extraction_class="subsector_en", extraction_text="Assorted basic social services"),  
            lx.data.Extraction(extraction_class="target_en", extraction_text="By 2030, ensure that all men and women, in particular the poor and the vulnerable, have equal rights to economic resources, as well as access to basic services, ownership and control over land and other forms of property, inheritance, natural resources, a")           
        ]
    ),
    lx.data.ExampleData(
        text="YEAR: 2018 EMERGENCY_TITLE: لايوجد TITLE_EN: Iftar DESC_EN: Iftar TITLE_AR: إفطار صائم DESC_AR: إفطار صائمم",
        extractions=[
            lx.data.Extraction(extraction_class="subsector_en", extraction_text="Seasonal programmes"),  
            lx.data.Extraction(extraction_class="target_en", extraction_text="Strengthen efforts to protect and safeguard the world’s cultural and natural heritage")           
        ]
    ),
    lx.data.ExampleData(
        text="YEAR: 2024 EMERGENCY_TITLE: Iraq (2024) TITLE_EN: Iraq Shelter Support for 3 Shelters in 2024 DESC_EN: Iraq Shelter Support for 3 Shelters in 2024 TITLE_AR: دعم ملاجئ العراق لعدد 3 ملاجئ لعام 2024 DESC_AR: دعم ملاجئ العراق لعدد 3 ملاجئ لعام 2024",
        extractions=[
            lx.data.Extraction(extraction_class="subsector_en", extraction_text="Emergency shelter and non-food items"),  
            lx.data.Extraction(extraction_class="target_en", extraction_text="By 2030, build the resilience of the poor and those in vulnerable situations and reduce their exposure and vulnerability to climate-related extreme events and other economic, social and environmental shocks and disasters")           
        ]
    )
]