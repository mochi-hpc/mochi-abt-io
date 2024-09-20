/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "abt-io.h"
#include <bedrock/AbstractComponent.hpp>

namespace tl = thallium;

class AbtIOComponent : public bedrock::AbstractComponent {

    abt_io_instance_id m_inst;

    public:

    AbtIOComponent(const abt_io_init_info* args)
    {
        m_inst = abt_io_init_ext(args);
        if(!m_inst) throw bedrock::Exception{
            "Could not initialize instance of ABT-IO"};
    }

    ~AbtIOComponent() {
        if(m_inst) abt_io_finalize(m_inst);
    }

    void* getHandle() override {
        return static_cast<void*>(m_inst);
    }

    std::string getConfig() override {
        auto config_cstr = abt_io_get_config(m_inst);
        auto config = std::string{config_cstr};
        free(config_cstr);
        return config;
    }

    static std::shared_ptr<bedrock::AbstractComponent>
        Register(const bedrock::ComponentArgs& args) {
            auto it = args.dependencies.find("pool");
            auto pool = it->second[0]->getHandle<tl::pool>();
            abt_io_init_info abt_io_args = {
                /* .json_config = */ args.config.c_str(),
                /* .progresss_pool = */ pool.native_handle()
            };
            return std::make_shared<AbtIOComponent>(&abt_io_args);
        }

    static std::vector<bedrock::Dependency>
        GetDependencies(const bedrock::ComponentArgs& args) {
            (void)args;
            std::vector<bedrock::Dependency> dependencies{
                bedrock::Dependency{
                    /* name */ "pool",
                    /* type */ "pool",
                    /* is_required */ true,
                    /* is_array */ false,
                    /* is_updatable */ false
                }
            };
            return dependencies;
        }
};

BEDROCK_REGISTER_COMPONENT_TYPE(abt_io, AbtIOComponent)
