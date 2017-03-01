
using Newtonsoft.Json;

public class USGRequest
{
    public string Url { get; set; }
    public Body Body { get; set; }
    public Headers Headers { get; set; }
}

public class USGCacheRequest
{
    public string Url { get; set; }
    public string Body { get; set; }
    public Headers Headers { get; set; }
}

public class Body
{
    public Georegion GeoRegion { get; set; }
    public string[] SupplierFamilies { get; set; }
    public string[] contentPrefs { get; set; }
}

public class Body1
{
    public Georegion1 GeoRegion { get; set; }
    public string[] SupplierFamilies { get; set; }
    public int[] contentPrefs { get; set; }
}

public class Georegion
{
    public Circle Circle { get; set; }
}
public class Georegion1
{
    public Circle1 Circle { get; set; }
}
public class Circle
{
    public Center Center { get; set; }
    public float RadiusKm { get; set; }
}

public class Circle1
{
    public Center1 Center { get; set; }
    public string RadiusKm { get; set; }
}

public class Center
{
    public float lat { get; set; }
    [JsonProperty(PropertyName = "long")]
    public float _long { get; set; }
}

public class Center1
{
    public string lat { get; set; }
    [JsonProperty(PropertyName = "long")]
    public string _long { get; set; }
}

public class Headers
{
    public string acceptlanguage { get; set; }
    public string oskitenantId { get; set; }
    public string oskicorrelationId { get; set; }
    public object oskiuserToken { get; set; }
}

public class Rootobject
{
    public Georegion geoRegion { get; set; }
    public string[] supplierFamilies { get; set; }
    public string[] contentPrefs { get; set; }
}


