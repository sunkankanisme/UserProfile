package cn.itcast.tags.web.service.impl;

import cn.itcast.tags.web.bean.dto.ModelDto;
import cn.itcast.tags.web.service.Engine;
import org.springframework.stereotype.Service;


@Service
public class EngineImpl implements Engine {
    @Override
    public void startModel(ModelDto model) {
        System.out.println(">>>>> TRY START OOZIE TASK ...");
    }

    @Override
    public void stopModel(ModelDto model) {

    }
}
