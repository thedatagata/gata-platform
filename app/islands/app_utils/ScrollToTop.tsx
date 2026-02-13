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
      class={`fixed bottom-6 right-6 p-3 bg-gata-green text-white rounded-full shadow-lg transition-opacity duration-300 hover:bg-gata-hover focus:outline-none ${isVisible ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
      aria-label="Scroll to top"
    >
      <svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M5 10l7-7m0 0l7 7m-7-7v18" /></svg>
    </button>
  );
}