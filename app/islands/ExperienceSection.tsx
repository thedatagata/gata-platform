import { useState } from "preact/hooks";

interface Experience {
  company: string;
  role: string;
  period: string;
  location: string;
  achievements: string[];
  stats?: { label: string; value: string }[];
}

export default function ExperienceSection() {
  const [activeIndex, setActiveIndex] = useState(0);

  const experiences: Experience[] = [
    {
      company: "PostPilot",
      role: "Data Platform Engineer",
      period: "02/2025 - 09/2025",
      location: "Raleigh, NC",
      achievements: [
        "Refactored 100 model dbt projects across 20 branches, reducing costs and accelerating development velocity.",
        "Delivered a production-ready identity graph solution in three months after previous efforts failed.",
        "Re-architected data pipelines utilizing DLT, DBT, and DuckDB to achieve Lakehouse Architecture.",
      ],
      stats: [
        { label: "Cloud Cost reduction", value: "40%" },
        { label: "Identity Graph Delivery", value: "3 Months" }
      ]
    },
    {
      company: "WillowTree",
      role: "Lead Technical Architect AEP",
      period: "03/2023 - 01/2025",
      location: "Raleigh, NC",
      achievements: [
        "Spearheaded Adobe Experience Platform implementations for eight enterprise clients.",
        "Accelerated AEP implementation timeline from six months to three months by automating data ingestion.",
        "Developed a library of open-source Python solutions to handle common platform obstacles.",
      ],
      stats: [
        { label: "Implementation Speed", value: "2x" },
        { label: "Enterprise Clients", value: "8" }
      ]
    },
    {
      company: "Heap Analytics",
      role: "Customer Solution Architect",
      period: "02/2021 - 02/2023",
      location: "Denver, CO",
      achievements: [
        "Generated $100K by developing a DBT service, automating model creation for data syncing.",
        "Achieved 90% retention rate of at-risk customers by partnering as technical champion.",
        "Collaborated with Product Marketing to generate $500K in pipeline through webinars.",
      ],
      stats: [
        { label: "Pipeline Generated", value: "$500K" },
        { label: "Retention Rate", value: "90%" }
      ]
    },
    {
      company: "DATA_GATA",
      role: "Analytics Engineering Consultant",
      period: "01/2019 - 02/2021",
      location: "Denver, CO",
      achievements: [
        "GTM Analytics Engineer @Slack: Introduced DBT to prevent redundant tables and simplify LookML.",
        "Sr Analytics Engineer @Vital Proteins: Solved attribution breakage using identity stitching.",
        "Sr Analytics Engineer @Aula: Developed DBT macros to automatically onboard new customers data.",
      ],
      stats: [
        { label: "Identity Stitching", value: "Solved" },
        { label: "Onboarding", value: "Automated" }
      ]
    }
  ];

  return (
    <section id="experience" class="py-32 bg-gata-cream relative overflow-hidden">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex flex-col md:flex-row gap-20">
          {/* Sidebar Navigation */}
          <div class="md:w-1/3 lg:w-1/4">
            <h2 class="text-[10px] font-black text-gata-green uppercase tracking-[0.5em] mb-12">Track Record</h2>
            <div class="space-y-4">
              {experiences.map((exp, i) => (
                <button
                  type="button"
                  key={exp.company}
                  onClick={() => setActiveIndex(i)}
                  class={`w-full text-left p-6 rounded-2xl transition-all duration-300 transform ${
                    activeIndex === i 
                    ? "bg-gata-dark text-gata-cream shadow-2xl scale-105 border-l-4 border-gata-green" 
                    : "bg-white text-gata-dark/40 hover:bg-gata-green/10 border-l-4 border-transparent"
                  }`}
                >
                  <p class="text-[10px] font-black uppercase tracking-widest mb-1 opacity-60">{exp.period}</p>
                  <h3 class="text-xl font-black italic tracking-tighter uppercase">{exp.company}</h3>
                </button>
              ))}
            </div>
          </div>

          {/* Main Experience Display */}
          <div class="flex-1">
            <div class="bg-white rounded-[3rem] p-8 md:p-16 shadow-[0_40px_100px_rgba(0,0,0,0.05)] border border-gata-dark/5 min-h-[600px] flex flex-col relative overflow-hidden">
              {/* Glossy overlay */}
              <div class="absolute top-0 right-0 w-64 h-64 bg-gata-green/5 rounded-full blur-3xl -mr-20 -mt-20" />
              
              <div class="relative z-10 flex-grow animate-fade-in-up" key={activeIndex}>
                <div class="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-12">
                   <div>
                    <h4 class="text-4xl md:text-5xl font-black text-gata-dark italic tracking-tighter uppercase mb-2">
                       {experiences[activeIndex].role}
                    </h4>
                    <p class="text-sm font-bold text-gata-green uppercase tracking-[0.2em]">
                      {experiences[activeIndex].company} • {experiences[activeIndex].location}
                    </p>
                   </div>
                   <div class="flex gap-4">
                      {experiences[activeIndex].stats?.map(stat => (
                        <div key={stat.label} class="bg-gata-dark/5 px-6 py-4 rounded-2xl text-center min-w-[120px]">
                          <p class="text-2xl font-black text-gata-green italic leading-none mb-1">{stat.value}</p>
                          <p class="text-[8px] font-black text-gata-dark/40 uppercase tracking-widest">{stat.label}</p>
                        </div>
                      ))}
                   </div>
                </div>

                <div class="space-y-8 mb-12">
                  {experiences[activeIndex].achievements.map((achievement, i) => (
                    <div key={i} class="flex gap-6 group">
                      <div class="mt-2 w-2 h-2 rounded-full bg-gata-green shrink-0 group-hover:scale-150 transition-transform" />
                      <p class="text-lg text-gata-dark/80 font-medium leading-relaxed">
                        {achievement}
                      </p>
                    </div>
                  ))}
                </div>

                <div class="mt-auto pt-12 border-t border-gata-dark/5">
                  <h5 class="text-[10px] font-black text-gata-dark/30 uppercase tracking-[0.3em] mb-6 italic">Engineering Toolkit Usage</h5>
                  <div class="flex flex-wrap gap-3">
                    {["DBT", "DuckDB", "DLT", "GCP", "Identity Stitching", "Lakehouse"].map(tag => (
                      <span key={tag} class="px-5 py-2 rounded-full border border-gata-dark/10 text-[10px] font-black text-gata-dark/60 uppercase tracking-widest">
                        {tag}
                      </span>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
