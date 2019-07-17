package com.cs.rfq.decorator;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;

public class Rfq implements Serializable {
    private String id;
    private String isin;
    private Long traderId;
    private Long entityId;
    private Long quantity;
    private Double price;
    private String side;
    private Long customerId;

    public static Rfq fromJson(String json) {
        //TODO: build a new RFQ setting all fields from data passed in the RFQ json message
        Type t = new TypeToken<Map<String, String>>() {
        }.getType();
        Map<String, String> fields = new Gson().fromJson(json, t);

        Rfq rfq = new Rfq();
        rfq.id = fields.get("id");
        rfq.isin = fields.get("instrumentId");
        rfq.traderId = Long.valueOf(fields.get("traderId"));
        rfq.entityId = Long.valueOf(fields.get("entityId"));
        rfq.quantity = Long.valueOf(fields.get("qty"));
        rfq.customerId = Long.valueOf(fields.get("customerId"));
        rfq.price = Double.valueOf(fields.get("price"));
        rfq.side = fields.get("side");
        return rfq;
    }

    @Override
    public String toString() {
        return "Rfq{" +
                "id='" + id + '\'' +
                ", isin='" + isin + '\'' +
                ", traderId=" + traderId +
                ", entityId=" + entityId +
                ", quantity=" + quantity +
                ", price=" + price +
                ", side=" + side +
                ", customerId=" + customerId +
                '}';
    }

    public boolean isBuySide() {
        return "B".equals(side);
    }

    public boolean isSellSide() {
        return "S".equals(side);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIsin() {
        return isin;
    }

    public void setIsin(String isin) {
        this.isin = isin;
    }

    public Long getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Long customerId) {
        this.customerId = customerId;
    }

    public Long getTraderId() {
        return traderId;
    }

    public void setTraderId(Long traderId) {
        this.traderId = traderId;
    }

    public Long getEntityId() {
        return entityId;
    }

    public void setEntityId(Long entityId) {
        this.entityId = entityId;
    }

    public Long getQuantity() {
        return quantity;
    }

    public void setQuantity(Long quantity) {
        this.quantity = quantity;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = side;
    }
}
