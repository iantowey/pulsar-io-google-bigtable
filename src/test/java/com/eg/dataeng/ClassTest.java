package com.eg.dataeng;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.demo.dataeng.BigTableSink;
import com.demo.dataeng.BigTableSinkMessage;
import lombok.Getter;
import lombok.ToString;
import org.instancio.Instancio;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.instancio.Select.field;

public class ClassTest {
    static ObjectMapper mapper = new ObjectMapper();

//    @Test
//    public void test() throws JsonProcessingException {
//        BigTableSinkMessage m = Instancio.of(BigTableSinkMessage.class)
//                .generate(field(BigTableSinkMessage::getRowKey), gen -> gen.oneOf("k1","k2","k3","k4","k5","k6","k7"))
//                .generate(field(BigTableSinkMessage::getRow), gen -> gen.map().withKeys("A", "D", "E", "F").maxSize(4).minSize(1))
//                .create();
//        System.out.println(mapper.writeValueAsString(m));
//    }

    @Test
    public void t1() throws IOException {
        String j ="{\"row\":{\"A\":{\"ZDVYF\":\"PCR\",\"ZRODDZ\":\"KEJXNSG\"},\"D\":{\"GKVHSQGJ\":\"TGIHQIWVLT\",\"OOOHXJXHMA\":\"XZKFZ\",\"BELJBGQTX\":\"QXR\",\"WQOQLLQLF\":\"TJTCO\",\"ASXZWEB\":\"RWSPUMZR\",\"DETTKEHY\":\"VWPKVUAH\"},\"E\":{\"HGPFRDH\":\"JQTTLHC\",\"DQQXVQXPJ\":\"SPBLQRS\",\"CNEIEAH\":\"REABB\",\"GTZORMRKBQ\":\"QIVBGBYVKC\",\"HUDNW\":\"VUJPUVW\",\"CBIXSZDT\":\"LFPKVNXY\"},\"F\":{\"JQORFYQ\":\"LAKDBD\",\"HPPC\":\"TMGHQVRC\",\"JEYG\":\"CVDL\"}},\"row_key\":\"k6\"}";
        BigTableSinkMessage message = mapper.readValue(j, BigTableSinkMessage.class);
        System.out.println(message.toString());
    }

    @Test
    public void t2() throws IOException {
        String j ="{\"k6\":{\"A\":{\"ZDVYF\":\"PCR\",\"ZRODDZ\":\"KEJXNSG\"},\"D\":{\"GKVHSQGJ\":\"TGIHQIWVLT\",\"OOOHXJXHMA\":\"XZKFZ\",\"BELJBGQTX\":\"QXR\",\"WQOQLLQLF\":null,\"ASXZWEB\":\"RWSPUMZR\",\"DETTKEHY\":\"VWPKVUAH\"},\"E\":{\"HGPFRDH\":\"JQTTLHC\",\"DQQXVQXPJ\":\"SPBLQRS\",\"CNEIEAH\":\"REABB\",\"GTZORMRKBQ\":\"QIVBGBYVKC\",\"HUDNW\":\"VUJPUVW\",\"CBIXSZDT\":\"LFPKVNXY\"},\"F\":{\"JQORFYQ\":\"LAKDBD\",\"HPPC\":\"TMGHQVRC\",\"JEYG\":\"CVDL\"}}}";
        Map.Entry<String,  Map<String, Map<String, String>>> message = mapper.readValue(j, new TypeReference<Map.Entry<String,  Map<String, Map<String, String>>>>(){});
        System.out.println(message.toString());
    }

//    private static Function<TaxonomyPropensityRecsLvx, BigTableSinkMessage> F = (taxonomyPropensityRecsLvx) -> BigTableSinkMessage.builder()
//            .rowKey(taxonomyPropensityRecsLvx.getCustomerId() + "#" + taxonomyPropensityRecsLvx.getVariant())
//            .row(
//                Map.ofEntries(
//                    Map.entry(
//                    "taxonomy-propensity-cols",
//                        Map.ofEntries(
//                            Map.entry("sorted_propensities",
//                                taxonomyPropensityRecsLvx.getPropensities().stream().map(Propensity::getTaxonomy).collect(Collectors.joining(","))
//                            )
//                        )
//                    )
//                )
//            ).build();

//    private static Function<BigTableSinkMessage, String> G = (t) -> {
//        try {
//            return mapper.writeValueAsString(t);
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException(e);
//        }
//    };

//    @Test
//    public void toBigtableEntry() throws IOException {
//        String s = "{\"date\":\"20230531\",\"customer_id\":170721165,\"variant\":\"MODEL_A\",\"propensities\":[{\"taxonomy\":\"furniture_nomattress\",\"score\":\"0.335862\"},{\"taxonomy\":\"home_improvement\",\"score\":\"0.12255087\"},{\"taxonomy\":\"kitchen_dining\",\"score\":\"0.11719994\"},{\"taxonomy\":\"garden_patio\",\"score\":\"0.10629349\"},{\"taxonomy\":\"living_room_furn\",\"score\":\"0.10405321\"},{\"taxonomy\":\"bedding_withmem_nomatt\",\"score\":\"0.09008071\"},{\"taxonomy\":\"lighting\",\"score\":\"0.078554615\"},{\"taxonomy\":\"bedroom_furniture_nomattress\",\"score\":\"0.06986822\"},{\"taxonomy\":\"homedecor_norugs\",\"score\":\"0.06696299\"},{\"taxonomy\":\"dining_furniture\",\"score\":\"0.06290536\"},{\"taxonomy\":\"Kitchen & Bath\",\"score\":\"0.050109442\"},{\"taxonomy\":\"decorative_accessories\",\"score\":\"0.04712213\"},{\"taxonomy\":\"sports_toys\",\"score\":\"0.03658849\"},{\"taxonomy\":\"office_furniture\",\"score\":\"0.02913839\"},{\"taxonomy\":\"kids_furniture\",\"score\":\"0.025446264\"},{\"taxonomy\":\"Rugs\",\"score\":\"0.024797\"},{\"taxonomy\":\"Holiday\",\"score\":\"0.018794036\"},{\"taxonomy\":\"artgallery\",\"score\":\"0.0133416075\"},{\"taxonomy\":\"window_treatments\",\"score\":\"0.0121638095\"},{\"taxonomy\":\"mattress_memoryfoam\",\"score\":\"0.011768556\"},{\"taxonomy\":\"jewelry_watches\",\"score\":\"0.0114279445\"},{\"taxonomy\":\"bathroom_furniture\",\"score\":\"0.007931664\"},{\"taxonomy\":\"clothing_shoes\",\"score\":\"0.0062217275\"},{\"taxonomy\":\"baby\",\"score\":\"0.0060373936\"},{\"taxonomy\":\"electronics\",\"score\":\"0.00219733\"},{\"taxonomy\":\"crafts\",\"score\":\"0.0012000281\"},{\"taxonomy\":\"luggage\",\"score\":\"0.0009149393\"},{\"taxonomy\":\"office_supplies\",\"score\":\"0.00054487627\"},{\"taxonomy\":\"pets\",\"score\":\"0.00024935338\"},{\"taxonomy\":\"food\",\"score\":\"3.877478e-05\"}]}";
//        TaxonomyPropensityRecsLvx message = mapper.readValue(s, TaxonomyPropensityRecsLvx.class);
//        System.out.println(message.toString());
//        String m = G.compose(F).apply(message);
//        System.out.println(m);
//    }
}


@Getter
@ToString
class TaxonomyPropensityRecsLvx{
    private String date;
    @JsonAlias("customer_id")
    private long customerId;
    private String variant;
    private List<Propensity> propensities;
}

@Getter
@ToString
class Propensity{
    private String taxonomy;
    private double score;
}