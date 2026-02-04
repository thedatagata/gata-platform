// islands/ScrollToTop.tsx
import { useEffect, useState } from "preact/hooks";

export default function ScrollToTop() {
  const [isVisible, setIsVisible] = useState(false);

  const toggleVisibility = () => {
    if (globalThis.scrollY > 300) {
      setIsVisible(true);
    } else {
      setIsVisible(false);
    }
  };

  const scrollToTop = () => {
    globalThis.scrollTo({
      top: 0,
      behavior: "smooth"
    });
  };

  useEffect(() => {
    globalThis.addEventListener("scroll", toggleVisibility);
    return () => globalThis.removeEventListener("scroll", toggleVisibility);
  }, []);

  return (
    <button 
      onClick={scrollToTop} 
      class={`fixed bottom-6 right-6 p-3 bg-[#90C137] text-white rounded-full shadow-lg transition-opacity duration-300 hover:bg-[#7dab2a] focus:outline-none ${isVisible ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
      aria-label="Scroll to top"
    >
      <i class="fas fa-arrow-up"></i>
    </button>
  );
}