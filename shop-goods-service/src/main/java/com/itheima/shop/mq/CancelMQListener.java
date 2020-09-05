package com.itheima.shop.mq;

import com.alibaba.fastjson.JSON;
import com.itheima.constant.ShopCode;
import com.itheima.entity.MQEntity;
import com.itheima.shop.mapper.TradeGoodsMapper;
import com.itheima.shop.mapper.TradeGoodsNumberLogMapper;
import com.itheima.shop.mapper.TradeMqConsumerLogMapper;
import com.itheima.shop.pojo.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Date;


@Slf4j
@Component
@RocketMQMessageListener(topic = "${mq.order.topic}",consumerGroup = "${mq.order.consumer.group.name}",messageModel = MessageModel.BROADCASTING )
public class CancelMQListener implements RocketMQListener<MessageExt>{


    @Value("${mq.order.consumer.group.name}")
    private String groupName;

    @Autowired
    private TradeGoodsMapper goodsMapper;

    @Autowired
    private TradeMqConsumerLogMapper mqConsumerLogMapper;

    @Autowired
    private TradeGoodsNumberLogMapper goodsNumberLogMapper;

    @Override
    public void onMessage(MessageExt messageExt) {
        String msgId=null;
        String tags=null;
        String keys=null;
        String body=null;
        try {
            //1. 解析消息内容
            msgId = messageExt.getMsgId();
            tags= messageExt.getTags();
            keys= messageExt.getKeys();
            body= new String(messageExt.getBody(),"UTF-8");

            log.info("接受消息成功");

            //2. 查询消息消费记录
            TradeMqConsumerLogKey primaryKey = new TradeMqConsumerLogKey();
            primaryKey.setMsgTag(tags);
            primaryKey.setMsgKey(keys);
            primaryKey.setGroupName(groupName);
            TradeMqConsumerLog tradeMqConsumerLog = mqConsumerLogMapper.selectByPrimaryKey(primaryKey);

            if(tradeMqConsumerLog!=null) {
                //3. 判断如果消费过...
                //3.1 获得消息处理状态
                Integer status = tradeMqConsumerLog.getConsumerStatus();
                //处理过...返回
              if (ShopCode.SHOP_MQ_MESSAGE_STATUS_SUCCESS.getCode().intValue()==status.intValue()) {
                 log.info("success"+msgId);
                 return;
              }
                //正在处理...返回
             if (ShopCode.SHOP_MQ_MESSAGE_STATUS_PROCESSING.getCode().intValue()==status.intValue()) {
             log.info("loading "+msgId);
             return;
             }
                //处理失败
             if (ShopCode.SHOP_MQ_MESSAGE_STATUS_FAIL.getCode().intValue()==status.intValue()) {
                 //获得消息处理次数
                 Integer consumerTimes = tradeMqConsumerLog.getConsumerTimes();
                 if (consumerTimes>3){
                     log.info("message"+msgId+"more 3 times");
                     return;
             }
                //使用数据库乐观锁更新
                 tradeMqConsumerLog.setConsumerStatus(ShopCode.SHOP_MQ_MESSAGE_STATUS_PROCESSING.getCode());
                 TradeMqConsumerLogExample example=new TradeMqConsumerLogExample();
                 TradeMqConsumerLogExample.Criteria criteria=example.createCriteria();
                 criteria.andMsgTagEqualTo(tradeMqConsumerLog.getMsgTag());
                 criteria.andMsgKeyEqualTo(tradeMqConsumerLog.getMsgKey());
                 criteria.andGroupNameEqualTo(groupName);
                 criteria.andConsumerTimesEqualTo(tradeMqConsumerLog.getConsumerTimes());
                 int r=mqConsumerLogMapper.updateByExampleSelective(tradeMqConsumerLog,example);
                 if (r<=0){
                     log.info("并法修改，稍后处理");
                 }
                //未修改成功,其他线程并发修改
            }else {
                //4. 判断如果没有消费过...
              tradeMqConsumerLog=new TradeMqConsumerLog();
                 tradeMqConsumerLog.setMsgTag(tags);
                 tradeMqConsumerLog.setMsgKey(keys);
                 tradeMqConsumerLog.setConsumerStatus(ShopCode.SHOP_MQ_MESSAGE_STATUS_PROCESSING.getCode());
                 tradeMqConsumerLog.setMsgBody(body);
                 tradeMqConsumerLog.setMsgId(msgId);
                 tradeMqConsumerLog.setConsumerTimes(0);
            }
                //将消息处理信息添加到数据库
                mqConsumerLogMapper.insert(tradeMqConsumerLog);
            }
            //5. 回退库存
            MQEntity mqEntity = JSON.parseObject(body, MQEntity.class);
            Long goodsId = mqEntity.getGoodsId();
            TradeGoods goods = goodsMapper.selectByPrimaryKey(goodsId);
            goods.setGoodsNumber(goods.getGoodsNumber()+mqEntity.getGoodsNum());
            goodsMapper.updateByPrimaryKey(goods);
            TradeGoodsNumberLog goodsNumberLog = new TradeGoodsNumberLog();

            goodsNumberLog.setOrderId(mqEntity.getOrderId());
           goodsNumberLog.setGoodsId(goodsId);
           goodsNumberLog.setGoodsNumber(mqEntity.getGoodsNum());
           goodsNumberLog.setLogTime(new Date());

           goodsNumberLogMapper.insert(goodsNumberLog);
            //6. 将消息的处理状态改为成功
            tradeMqConsumerLog.setConsumerStatus(ShopCode.SHOP_MQ_MESSAGE_STATUS_SUCCESS.getCode());
            tradeMqConsumerLog.setConsumerTimestamp(new Date());
            mqConsumerLogMapper.updateByPrimaryKey(tradeMqConsumerLog);

            log.info("回退库存成功");
        } catch (Exception e) {
            e.printStackTrace();
            TradeMqConsumerLogKey primaryKey = new TradeMqConsumerLogKey();
            primaryKey.setMsgTag(tags);
            primaryKey.setMsgKey(keys);
            primaryKey.setGroupName(groupName);
            TradeMqConsumerLog mqConsumerLog = mqConsumerLogMapper.selectByPrimaryKey(primaryKey);
            if(mqConsumerLog==null){
                //数据库未有记录
                mqConsumerLog = new TradeMqConsumerLog();
                mqConsumerLog.setMsgTag(tags);
                mqConsumerLog.setMsgKey(keys);
                mqConsumerLog.setGroupName(groupName);
                mqConsumerLog.setConsumerStatus(ShopCode.SHOP_MQ_MESSAGE_STATUS_FAIL.getCode());
                mqConsumerLog.setMsgBody(body);
                mqConsumerLog.setMsgId(msgId);
                mqConsumerLog.setConsumerTimes(1);
                mqConsumerLogMapper.insert(mqConsumerLog);
            }else{
                mqConsumerLog.setConsumerTimes(mqConsumerLog.getConsumerTimes()+1);
                mqConsumerLogMapper.updateByPrimaryKeySelective(mqConsumerLog);
            }
            MQEntity mqEntity=JSON.parseObject(body,MQEntity.class);
            Long goodsId=mqEntity.getGoodsId();
            TradeGoods goods=goodsMapper.selectByPrimaryKey(goodsId);
            goods.setGoodsNumber(goods.getGoodsNumber()+mqEntity.getGoodsNum());

        }

    }
}
